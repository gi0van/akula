use super::headers::{
    header::BlockHeader,
    header_slice_status_watch::HeaderSliceStatusWatch,
    header_slices,
    header_slices::{HeaderSlice, HeaderSliceStatus, HeaderSlices},
};
use crate::{
    kv,
    kv::{
        mdbx::*,
        tables::{self, HeaderKey},
    },
    models::*,
};
use anyhow::format_err;
use parking_lot::RwLock;
use std::{
    ops::{ControlFlow, DerefMut},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::Mutex as AsyncMutex;
use tracing::*;

#[derive(Copy, Clone)]
pub enum SaveOrder {
    Monotonic,
    Random,
}

pub struct SaveMonotonicCursors<'tx> {
    cursor_for_header_table: MdbxCursor<'tx, RW, kv::tables::Header>,
    cursor_for_canonical_header_table: MdbxCursor<'tx, RW, kv::tables::CanonicalHeader>,
    cursor_for_total_difficulty_table: MdbxCursor<'tx, RW, kv::tables::HeadersTotalDifficulty>,
}

/// Saves slices into the database, and sets Saved status.
pub struct SaveStage<'tx, 'db: 'tx, E>
where
    E: EnvironmentKind,
{
    header_slices: Arc<HeaderSlices>,
    db_transaction: &'tx MdbxTransaction<'db, RW, E>,
    order: SaveOrder,
    is_canonical_chain: bool,
    monotonic_save_cursors: Option<AsyncMutex<SaveMonotonicCursors<'tx>>>,
    pending_watch: HeaderSliceStatusWatch,
    remaining_count: Arc<AtomicUsize>,
}

impl<'tx, 'db: 'tx, E> SaveStage<'tx, 'db, E>
where
    E: EnvironmentKind,
{
    pub fn new(
        header_slices: Arc<HeaderSlices>,
        db_transaction: &'tx MdbxTransaction<'db, RW, E>,
        order: SaveOrder,
        is_canonical_chain: bool,
    ) -> Self {
        Self {
            header_slices: header_slices.clone(),
            db_transaction,
            order,
            is_canonical_chain,
            monotonic_save_cursors: None,
            pending_watch: HeaderSliceStatusWatch::new(
                HeaderSliceStatus::Verified,
                header_slices,
                "SaveStage",
            ),
            remaining_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        // initially remaining_count = 0, so we wait for any verified slices to try to save them
        // since we want to save headers sequentially, there might be some remaining slices
        // in this case we wait until some more slices become verified
        // hopefully its the slices at the front so that we can save them
        self.pending_watch
            .wait_while(self.get_remaining_count())
            .await?;

        let pending_count = self.pending_watch.pending_count();

        debug!("SaveStage: saving {} slices", pending_count);
        let saved_count = match self.order {
            SaveOrder::Monotonic => self.save_pending_monotonic(pending_count)?,
            SaveOrder::Random => self.save_pending_all(pending_count)?,
        };
        debug!("SaveStage: saved {} slices", saved_count);

        self.set_remaining_count(pending_count - saved_count);

        Ok(())
    }

    fn get_remaining_count(&self) -> usize {
        self.remaining_count.load(Ordering::SeqCst)
    }

    fn set_remaining_count(&self, value: usize) {
        self.remaining_count.store(value, Ordering::SeqCst);
    }

    pub fn can_proceed_check(&self) -> Box<dyn Fn() -> bool + Send> {
        let header_slices = self.header_slices.clone();
        let remaining_count = self.remaining_count.clone();
        let check = move || -> bool {
            header_slices.count_slices_in_status(HeaderSliceStatus::Verified)
                != remaining_count.load(Ordering::SeqCst)
        };
        Box::new(check)
    }

    fn save_pending_monotonic(&mut self, pending_count: usize) -> anyhow::Result<usize> {
        if self.monotonic_save_cursors.is_none() {
            self.monotonic_save_cursors =
                Some(AsyncMutex::new(self.make_monotonic_save_cursors()?));
        }
        let mut saved_count: usize = 0;
        for _ in 0..pending_count {
            let next_slice_lock = self.find_next_pending_monotonic();

            if let Some(slice_lock) = next_slice_lock {
                self.save_slice(slice_lock)?;
                saved_count += 1;
            } else {
                break;
            }
        }
        Ok(saved_count)
    }

    fn find_next_pending_monotonic(&self) -> Option<Arc<RwLock<HeaderSlice>>> {
        let initial_value = Option::<Arc<RwLock<HeaderSlice>>>::None;
        let next_slice_lock = self.header_slices.try_fold(initial_value, |_, slice_lock| {
            let slice = slice_lock.read();
            match slice.status {
                HeaderSliceStatus::Saved => ControlFlow::Continue(None),
                HeaderSliceStatus::Verified => ControlFlow::Break(Some(slice_lock.clone())),
                _ => ControlFlow::Break(None),
            }
        });

        if let ControlFlow::Break(slice_lock_opt) = next_slice_lock {
            slice_lock_opt
        } else {
            None
        }
    }

    fn save_pending_all(&self, pending_count: usize) -> anyhow::Result<usize> {
        let mut saved_count: usize = 0;
        while let Some(slice_lock) = self
            .header_slices
            .find_by_status(HeaderSliceStatus::Verified)
        {
            // don't update more than asked
            if saved_count >= pending_count {
                break;
            }

            self.save_slice(slice_lock)?;
            saved_count += 1;
        }
        Ok(saved_count)
    }

    fn save_slice(&self, slice_lock: Arc<RwLock<HeaderSlice>>) -> anyhow::Result<()> {
        // take out the headers, and unlock the slice while save_slice is in progress
        let mut headers = {
            let mut slice = slice_lock.write();
            slice.headers.take().ok_or_else(|| {
                format_err!("SaveStage: inconsistent state - Verified slice has no headers")
            })?
        };

        // Don't save more than HEADER_SLICE_SIZE headers.
        // Preverified phase is downloading HEADER_SLICE_SIZE + 1 headers,
        // because an extra header is needed for verification.
        // The next slice starts from the same header: prev_slice[last] = next_slice[first]
        // We don't want to re-save it twice and mess up the monotonic save.
        headers.truncate(header_slices::HEADER_SLICE_SIZE);

        self.save_headers(&headers)?;

        let mut slice = slice_lock.write();

        // put the detached headers back
        slice.headers = Some(headers);

        self.header_slices
            .set_slice_status(slice.deref_mut(), HeaderSliceStatus::Saved);
        Ok(())
    }

    fn save_headers(&self, headers: &[BlockHeader]) -> anyhow::Result<()> {
        let tx = &self.db_transaction;
        for header_ref in headers {
            // this clone happens mostly on the stack (except extra_data)
            let header = header_ref.clone();

            Self::save_header(
                header,
                self.is_canonical_chain,
                self.monotonic_save_cursors.as_ref(),
                tx,
            )?;
        }
        Ok(())
    }

    fn read_parent_header_total_difficulty(
        child: &BlockHeader,
        tx: &'tx MdbxTransaction<'db, RW, E>,
    ) -> anyhow::Result<Option<U256>> {
        if child.number() == BlockNumber(0) {
            return Ok(Some(U256::ZERO));
        }
        let parent_block_num = BlockNumber(child.number().0 - 1);
        let parent_header_key: HeaderKey = (parent_block_num, child.parent_hash());
        let parent_total_difficulty = tx.get(tables::HeadersTotalDifficulty, parent_header_key)?;
        Ok(parent_total_difficulty)
    }

    fn header_total_difficulty(
        header: &BlockHeader,
        tx: &'tx MdbxTransaction<'db, RW, E>,
    ) -> anyhow::Result<Option<U256>> {
        let Some(parent_total_difficulty) = Self::read_parent_header_total_difficulty(header, tx)? else {
            return Ok(None)
        };
        let total_difficulty = parent_total_difficulty + header.difficulty();
        Ok(Some(total_difficulty))
    }

    pub fn load_canonical_header_by_num(
        block_num: BlockNumber,
        tx: &'tx MdbxTransaction<'db, RW, E>,
    ) -> anyhow::Result<Option<BlockHeader>> {
        let Some(header_hash) = tx.get(tables::CanonicalHeader, block_num)? else {
            return Ok(None);
        };
        let header_key: HeaderKey = (block_num, header_hash);
        let header_opt = tx.get(tables::Header, header_key)?;
        Ok(header_opt.map(|header| BlockHeader::new(header, header_hash)))
    }

    pub fn save_header(
        header: BlockHeader,
        is_canonical_chain: bool,
        monotonic_save_cursors: Option<&AsyncMutex<SaveMonotonicCursors<'tx>>>,
        tx: &'tx MdbxTransaction<'db, RW, E>,
    ) -> anyhow::Result<()> {
        let block_num = header.number();
        let header_hash = header.hash();
        let header_key: HeaderKey = (block_num, header_hash);

        if is_canonical_chain {
            Self::update_canonical_chain_header(&header, monotonic_save_cursors, tx)?;
        }

        if let Some(cursors_lock) = monotonic_save_cursors {
            let mut cursors = cursors_lock.try_lock()?;
            let cursor = &mut cursors.cursor_for_header_table;
            cursor.append(header_key, header.header)?
        } else {
            tx.set(kv::tables::Header, header_key, header.header)?
        }

        tx.set(kv::tables::HeaderNumber, header_hash, block_num)?;

        Ok(())
    }

    pub fn update_canonical_chain_header(
        header: &BlockHeader,
        monotonic_save_cursors: Option<&AsyncMutex<SaveMonotonicCursors<'tx>>>,
        tx: &'tx MdbxTransaction<'db, RW, E>,
    ) -> anyhow::Result<()> {
        let block_num = header.number();
        let header_hash = header.hash();
        let header_key: HeaderKey = (block_num, header_hash);

        if let Some(cursors_lock) = monotonic_save_cursors {
            let mut cursors = cursors_lock.try_lock()?;
            let cursor = &mut cursors.cursor_for_canonical_header_table;
            cursor.append(block_num, header_hash)?
        } else {
            tx.set(kv::tables::CanonicalHeader, block_num, header_hash)?
        }

        tx.set(kv::tables::LastHeader, (), header_hash)?;

        let total_difficulty_opt = Self::header_total_difficulty(header, tx)?;
        if let Some(total_difficulty) = total_difficulty_opt {
            if let Some(cursors_lock) = monotonic_save_cursors {
                let mut cursors = cursors_lock.try_lock()?;
                let cursor = &mut cursors.cursor_for_total_difficulty_table;
                cursor.append(header_key, total_difficulty)?
            } else {
                tx.set(
                    kv::tables::HeadersTotalDifficulty,
                    header_key,
                    total_difficulty,
                )?
            }
        }

        Ok(())
    }

    fn make_monotonic_save_cursors(&self) -> anyhow::Result<SaveMonotonicCursors<'tx>> {
        let cursor_for_header_table = self.db_transaction.cursor(kv::tables::Header)?;
        let cursor_for_canonical_header_table =
            self.db_transaction.cursor(kv::tables::CanonicalHeader)?;
        let cursor_for_total_difficulty_table = self
            .db_transaction
            .cursor(kv::tables::HeadersTotalDifficulty)?;
        let cursors = SaveMonotonicCursors {
            cursor_for_header_table,
            cursor_for_canonical_header_table,
            cursor_for_total_difficulty_table,
        };
        Ok(cursors)
    }

    pub fn unwind(
        unwind_to_block_num: BlockNumber,
        tx: &'tx MdbxTransaction<'db, RW, E>,
    ) -> anyhow::Result<()> {
        // headers after unwind_to_block_num are not canonical anymore
        for i in unwind_to_block_num.0 + 1.. {
            let num = BlockNumber(i);
            let was_found = tx.del(tables::CanonicalHeader, num, None)?;
            if !was_found {
                break;
            }
        }

        // update LastHeader to point to unwind_to_block_num
        let last_header_hash_opt = tx.get(tables::CanonicalHeader, unwind_to_block_num)?;
        if let Some(hash) = last_header_hash_opt {
            tx.set(tables::LastHeader, (), hash)?;
        } else {
            anyhow::bail!(
                "unwind: not found header hash of the top block after unwind {}",
                unwind_to_block_num.0
            );
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl<'tx, 'db: 'tx, E> super::stage::Stage for SaveStage<'tx, 'db, E>
where
    E: EnvironmentKind,
{
    async fn execute(&mut self) -> anyhow::Result<()> {
        Self::execute(self).await
    }
    fn can_proceed_check(&self) -> Box<dyn Fn() -> bool + Send> {
        Self::can_proceed_check(self)
    }
}
