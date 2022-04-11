use crate::{
    kv::{
        mdbx::MdbxTransaction,
        tables::{self, TruncateStart},
    },
    models::*,
    p2p::{
        peer::*,
        types::{Message, Status},
    },
    stagedsync::{stage::*, stages::BODIES},
    StageId,
};
use async_trait::async_trait;
use futures_util::{future, stream::FuturesUnordered, FutureExt, StreamExt};
use hashbrown::HashMap;
use hashlink::LinkedHashMap;
use mdbx::{EnvironmentKind, RO, RW};
use rayon::{
    iter::{ParallelDrainRange, ParallelIterator},
    slice::ParallelSliceMut,
};
use std::{iter::Iterator, sync::Arc, time::Duration};
use tracing::*;

#[derive(Debug)]
pub struct BodyDownload {
    /// Peer is a interface for interacting with p2p.
    peer: Arc<Peer>,
    /// Requests is a mapping of (ommers_hash, transactions_root) to hash with it's header.
    requests: LinkedHashMap<(H256, H256), (H256, BlockNumber)>,
}

#[async_trait]
impl<'db, E> Stage<'db, E> for BodyDownload
where
    E: EnvironmentKind,
{
    fn id(&self) -> StageId {
        BODIES
    }

    async fn execute<'tx>(
        &mut self,
        txn: &'tx mut MdbxTransaction<'db, RW, E>,
        input: StageInput,
    ) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let prev_progress = input.stage_progress.map(|v| v + 1u8).unwrap_or_default();
        let starting_block = prev_progress;
        let target = input.previous_stage.map(|(_, v)| v).unwrap();

        let mut stream = self.peer.recv_bodies().await?;
        self.collect_bodies(&mut stream, txn, starting_block, target)
            .await?;

        Ok(ExecOutput::Progress {
            stage_progress: target,
            done: true,
        })
    }

    async fn unwind<'tx>(
        &mut self,
        txn: &'tx mut MdbxTransaction<'db, RW, E>,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        let mut block_body_cur = txn.cursor(tables::BlockBody)?;
        let mut block_tx_cur = txn.cursor(tables::BlockTransaction)?;

        while let Some(((number, _), body)) = block_body_cur.last()? {
            if number <= input.unwind_to {
                break;
            }

            block_body_cur.delete_current()?;
            let mut deleted = 0;
            while deleted < body.tx_amount {
                let to_delete = body.base_tx_id + deleted;
                if block_tx_cur.seek_exact(to_delete)?.is_some() {
                    block_tx_cur.delete_current()?;
                }

                deleted += 1;
            }
        }

        Ok(UnwindOutput {
            stage_progress: input.unwind_to,
        })
    }
}

impl BodyDownload {
    pub fn new<T, E>(
        conn: T,
        chain_config: ChainConfig,
        txn: MdbxTransaction<'_, RO, E>,
    ) -> anyhow::Result<Self>
    where
        T: Into<SentryPool>,
        E: EnvironmentKind,
    {
        let (height, hash) = txn.cursor(tables::CanonicalHeader)?.last()?.unwrap();
        Ok(Self {
            peer: Arc::new(Peer::new(
                conn,
                chain_config,
                Status {
                    height,
                    hash,
                    total_difficulty: txn
                        .get(tables::HeadersTotalDifficulty, (height, hash))?
                        .map(|td| td.to_be_bytes().into())
                        .unwrap_or_default(),
                },
            )),
            requests: Default::default(),
        })
    }

    async fn collect_bodies<E: EnvironmentKind>(
        &mut self,
        stream: &mut InboundStream,
        txn: &mut MdbxTransaction<'_, RW, E>,
        starting_block: BlockNumber,
        target: BlockNumber,
    ) -> anyhow::Result<()> {
        self.prepare_requests(txn, starting_block, target)?;
        let mut ticker = tokio::time::interval(Duration::from_secs(30));
        let mut bodies =
            HashMap::<BlockNumber, (H256, BlockBody)>::with_capacity(self.requests.len());

        while !self.requests.is_empty() {
            match future::select(stream.next().fuse(), ticker.tick().boxed().fuse()).await {
                future::Either::Left((Some(msg), _)) => match msg.msg {
                    Message::BlockBodies(msg) => {
                        for body in msg.bodies {
                            if let Some((hash, number)) = self
                                .requests
                                .remove(&(body.ommers_hash(), body.transactions_root()))
                            {
                                let _v = bodies.insert(number, (hash, body));
                                debug_assert!(_v.is_none());
                            }
                        }
                    }
                    other => debug!("Other: {:?}", other),
                },
                _ => {
                    self.requests
                        .values()
                        .cloned()
                        .map(|(h, _)| h)
                        .collect::<Vec<_>>()
                        .chunks(CHUNK_SIZE)
                        .map(|chunk| {
                            let peer = self.peer.clone();
                            let chunk = chunk.to_vec();
                            tokio::spawn(async move { peer.send_body_request(chunk).await })
                        })
                        .collect::<FuturesUnordered<_>>()
                        .for_each(async move |f| drop(f))
                        .await;
                }
            }
        }

        for number in starting_block..=target {
            if !bodies.contains_key(&number) {
                bodies.insert(
                    number,
                    (
                        txn.get(tables::CanonicalHeader, number).unwrap().unwrap(),
                        BlockBody::default(),
                    ),
                );
            }
        }

        let mut bodies = bodies.into_iter().collect::<Vec<_>>();
        bodies.par_sort_unstable_by_key(|(v, _)| **v);
        let block_bodies = bodies
            .par_drain(..)
            .map(|(number, (hash, mut body))| {
                let txs = body
                    .transactions
                    .par_drain(..)
                    .map(|transaction| (transaction.hash(), transaction))
                    .collect::<Vec<_>>();
                let tx_amount = txs.len() as u64;
                (
                    number,
                    hash,
                    txs,
                    BodyForStorage {
                        base_tx_id: TxIndex(0),
                        tx_amount,
                        uncles: body.ommers,
                    },
                )
            })
            .collect::<Vec<_>>();

        let mut cursor = txn.cursor(tables::BlockBody)?;
        let mut block_tx_cursor = txn.cursor(tables::BlockTransaction)?;
        let mut lookup_cursor = txn.cursor(tables::BlockTransactionLookup)?;
        let mut base_tx_id = cursor
            .last()?
            .map(|((_, _), body)| body.base_tx_id.0 + body.tx_amount)
            .unwrap();

        for (block_number, hash, transactions, mut body) in block_bodies {
            if block_number == 0 {
                continue;
            }
            debug!(
                "Inserting block_number={} hash={:?} base_tx_id={} transactions={} uncles={}",
                block_number,
                hash,
                base_tx_id,
                transactions.len(),
                body.uncles.len()
            );
            body.base_tx_id = TxIndex(base_tx_id);
            cursor.put((block_number, hash), body)?;
            for (hash, transaction) in transactions {
                lookup_cursor.put(hash, TruncateStart(block_number))?;
                block_tx_cursor.put(TxIndex(base_tx_id), transaction)?;
                base_tx_id += 1;
            }
        }

        Ok(())
    }

    pub fn prepare_requests<E: EnvironmentKind>(
        &mut self,
        txn: &mut MdbxTransaction<'_, RW, E>,
        starting_block: BlockNumber,
        target: BlockNumber,
    ) -> anyhow::Result<()> {
        self.requests = txn
            .cursor(tables::CanonicalHeader)?
            .walk(Some(starting_block))
            .filter_map(Result::ok)
            .filter_map(Option::Some)
            .map_while(|(n, h)| {
                (n <= target).then(|| {
                    let header = txn.get(tables::Header, (n, h)).unwrap().unwrap();
                    (
                        (header.ommers_hash, header.transactions_root),
                        (h, header.number),
                    )
                })
            })
            .filter(|&((ommers_hash, transactions_root), _)| {
                !(ommers_hash == EMPTY_LIST_HASH && transactions_root == EMPTY_ROOT)
            })
            .collect::<LinkedHashMap<_, _>>();
        Ok(())
    }
}
