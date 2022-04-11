use super::helpers;
use crate::{
    kv::{
        mdbx::*,
        tables::{self, BitmapKey},
        MdbxWithDirHandle,
    },
    models::*,
};
use anyhow::format_err;
use async_trait::async_trait;
use croaring::Treemap;
use ethereum_jsonrpc::{
    types, BlockData, BlockDetails, BlockTransactions, ContractCreatorData, InternalOperation,
    Issuance, OtterscanApiServer, TraceEntry, TransactionsWithReceipts,
};
use jsonrpsee::core::RpcResult;
use std::sync::Arc;
use tokio::pin;

pub struct OtterscanApiServerImpl<SE>
where
    SE: EnvironmentKind,
{
    pub db: Arc<MdbxWithDirHandle<SE>>,
}

impl<DB> OtterscanApiServerImpl<DB>
where
    DB: EnvironmentKind,
{
    fn get_block_details_inner<K: TransactionKind, E: EnvironmentKind>(
        &self,
        tx: &MdbxTransaction<'_, K, E>,
        block_id: impl Into<types::BlockId>,
        include_txs: bool,
    ) -> RpcResult<Option<BlockDetails>> {
        if let Some(block) = helpers::construct_block(tx, block_id, include_txs, None)? {
            let mut details = BlockDetails {
                block: BlockData {
                    transaction_count: block.transactions.len() as u64,
                    inner: block,
                },
                issuance: Issuance {
                    block_reward: U256::ZERO,
                    uncle_reward: U256::ZERO,
                    issuance: U256::ZERO,
                },
                total_fees: U256::ZERO,
            };

            if !include_txs {
                details.block.inner.transactions.clear();
            }
            return Ok(Some(details));
        }

        Ok(None)
    }
}

#[async_trait]
impl<DB> OtterscanApiServer for OtterscanApiServerImpl<DB>
where
    DB: EnvironmentKind,
{
    async fn get_api_level(&self) -> RpcResult<u8> {
        Ok(8)
    }
    async fn get_internal_operations(&self, hash: H256) -> RpcResult<Vec<InternalOperation>> {
        let _ = hash;
        Ok(vec![])
    }
    async fn search_transactions_before(
        &self,
        addr: Address,
        mut block_num: u64,
        page_size: usize,
    ) -> RpcResult<TransactionsWithReceipts> {
        let dbtx = self.db.begin()?;

        let call_from_cursor = dbtx.cursor(tables::CallFromIndex)?;
        let call_to_cursor = dbtx.cursor(tables::CallToIndex)?;

        let chain_config = dbtx
            .get(tables::Config, ())?
            .ok_or_else(|| format_err!("chain spec not found"))?;

        let first_page = if block_num == 0 {
            true
        } else {
            // Internal search code considers blockNum [including], so adjust the value
            block_num -= 1;

            false
        };

        // Initialize search cursors at the first shard >= desired block number
        let call_from_provider =
            CallCursorBackwardBlockProvider::new(call_from_cursor, addr, block_num);
        let call_to_provider =
            CallCursorBackwardBlockProvider::new(call_to_cursor, addr, block_num);
        let call_from_to_provider =
            CallFromToBlockProvider::new(false, call_from_provider, call_to_provider);

        let mut txs = Vec::with_capacity(page_size);
        let mut receipts = Vec::with_capacity(page_size);

        let mut result_count = 0;
        let mut has_more = true;
        loop {
            if result_count >= page_size || !has_more {
                break;
            }

            let mut results;
            (results, has_more) = self.trace_blocks(
                addr,
                chain_config,
                page_size,
                result_count,
                call_from_to_provider,
            )?;

            for r in results {
                result_count += r.txs.len();
                for tx in r.txs.into_iter().rev() {
                    txs.push(tx)
                }
                for receipt in r.receipts.into_iter().rev() {
                    receipts.push(receipt);
                }

                if result_count >= page_size {
                    break;
                }
            }
        }

        Ok(TransactionsWithReceipts {
            txs,
            receipts,
            first_page,
            last_page: !has_more,
        })
    }
    async fn search_transactions_after(
        &self,
        addr: Address,
        block_num: u64,
        page_size: usize,
    ) -> RpcResult<TransactionsWithReceipts> {
        let _ = addr;
        let _ = block_num;
        let _ = page_size;
        Ok(TransactionsWithReceipts {
            txs: vec![],
            receipts: vec![],
            first_page: true,
            last_page: true,
        })
    }
    async fn get_block_details(&self, number: u64) -> RpcResult<Option<BlockDetails>> {
        self.get_block_details_inner(
            &self.db.begin()?,
            types::BlockNumber::Number(number.into()),
            false,
        )
    }
    async fn get_block_details_by_hash(&self, hash: H256) -> RpcResult<Option<BlockDetails>> {
        self.get_block_details_inner(&self.db.begin()?, hash, false)
    }
    async fn get_block_transactions(
        &self,
        number: u64,
        page_number: usize,
        page_size: usize,
    ) -> RpcResult<Option<BlockTransactions>> {
        let tx = self.db.begin()?;
        if let Some(mut block_details) =
            self.get_block_details_inner(&tx, types::BlockNumber::Number(number.into()), true)?
        {
            let page_end = block_details
                .block
                .inner
                .transactions
                .len()
                .saturating_sub(page_number * page_size);
            let page_start = page_end.saturating_sub(page_size);

            block_details.block.inner.transactions = block_details
                .block
                .inner
                .transactions
                .get(page_start..page_end)
                .map(|v| v.to_vec())
                .unwrap_or_default();

            return Ok(Some(BlockTransactions {
                receipts: helpers::get_receipts(&tx, number.into())?,
                fullblock: block_details.block.inner,
            }));
        }

        Ok(None)
    }
    async fn has_code(&self, address: Address, block_id: types::BlockId) -> RpcResult<bool> {
        let tx = self.db.begin()?;

        if let Some((block_number, _)) = helpers::resolve_block_id(&tx, block_id)? {
            if let Some(account) =
                crate::accessors::state::account::read(&tx, address, Some(block_number))?
            {
                return Ok(account.code_hash != EMPTY_HASH);
            }
        }

        Ok(false)
    }
    async fn trace_transaction(&self, hash: H256) -> RpcResult<Vec<TraceEntry>> {
        let _ = hash;
        Ok(vec![])
    }
    async fn get_transaction_error(&self, hash: H256) -> RpcResult<types::Bytes> {
        let _ = hash;
        Ok(types::Bytes::default())
    }
    async fn get_transaction_by_sender_and_nonce(
        &self,
        addr: Address,
        nonce: u64,
    ) -> RpcResult<Option<H256>> {
        let _ = addr;
        let _ = nonce;

        let tx = self.db.begin()?;

        let acc_history_cursor = tx.cursor(tables::AccountHistory)?;
        let mut acc_change_cursor = tx.cursor(tables::AccountChangeSet)?;

        // Locate the chunk where the nonce happens
        let mut max_bl_prev_chunk = BlockNumber(0);
        let mut bitmap = Treemap::default();
        let mut acc = None;

        let walker = acc_history_cursor.walk(Some(BitmapKey {
            inner: addr,
            block_number: 0.into(),
        }));
        pin!(walker);

        while let Some((BitmapKey { inner, .. }, b)) = walker.next().transpose()? {
            if inner != addr {
                break;
            }

            bitmap = b;

            // Inspect block changeset
            let max_bl = bitmap.maximum().unwrap_or(0);

            if let Some(a) = acc_change_cursor
                .find_account(max_bl.into(), addr)?
                .ok_or_else(|| format_err!("account not found"))?
            {
                if a.nonce > nonce {
                    acc = Some(a);
                    break;
                }
            }

            max_bl_prev_chunk = BlockNumber(max_bl);
        }

        if acc.is_none() {
            // Check plain state
            if let Some(a) = tx.get(tables::Account, addr)? {
                // Nonce changed in plain state, so it means the last block of last chunk
                // contains the actual nonce change
                if a.nonce > nonce {
                    acc = Some(a);
                }
            }
        }

        Ok(if acc.is_some() {
            // Locate the exact block inside chunk when the nonce changed
            let blocks = bitmap.iter().collect::<Vec<_>>();
            let mut idx = 0;
            for (i, block) in blocks.iter().enumerate() {
                // Locate the block changeset
                if let Some(acc) = acc_change_cursor
                    .find_account((*block).into(), addr)?
                    .ok_or_else(|| format_err!("account not found"))?
                {
                    // Since the state contains the nonce BEFORE the block changes, we look for
                    // the block when the nonce changed to be > the desired once, which means the
                    // previous history block contains the actual change; it may contain multiple
                    // nonce changes.
                    if acc.nonce > nonce {
                        idx = i;
                        break;
                    }
                }
            }

            // Since the changeset contains the state BEFORE the change, we inspect
            // the block before the one we found; if it is the first block inside the chunk,
            // we use the last block from prev chunk
            let nonce_block = if idx > 0 {
                BlockNumber(blocks[idx - 1])
            } else {
                max_bl_prev_chunk
            };

            let hash = crate::accessors::chain::canonical_hash::read(&tx, nonce_block)?
                .ok_or_else(|| format_err!("canonical hash not found for block {nonce_block}"))?;
            let txs =
                crate::accessors::chain::block_body::read_without_senders(&tx, hash, nonce_block)?
                    .ok_or_else(|| format_err!("body not found for block {nonce_block}"))?
                    .transactions;

            Some({
                txs
                    .into_iter()
                    .find(|t| t.nonce() == nonce)
                    .ok_or_else(|| format_err!("body for block #{nonce_block} does not contain tx for {addr}/#{nonce} despite indications for otherwise"))?
                    .hash()
            })
        } else {
            None
        })
    }
    async fn get_contract_creator(&self, addr: Address) -> RpcResult<Option<ContractCreatorData>> {
        let _ = addr;
        Ok(None)
    }
}
