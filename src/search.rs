#[derive(Debug, Clone)]
pub(crate) struct BPTreeFileIndex {
    file: File,
    header: IndexHeader,
    metadata: TreeMeta,
    root_node: [u8; BLOCK_SIZE],
}

struct Node {/* fields omitted */}

impl Node {
    pub(super) fn key_offset_serialized(buf: &[u8], key: &[u8]) -> Result<u64> {
        let meta_size = NodeMeta::serialized_size_default()? as usize;
        let node_size = deserialize::<NodeMeta>(&buf[..meta_size])?.size as usize;
        let offsets_offset = meta_size + node_size * key.len();
        let ind = match Self::binary_search_serialized(key, &buf[meta_size..offsets_offset]) {
            Ok(pos) => pos + 1,
            Err(pos) => pos,
        };
        let offset = offsets_offset + ind * size_of::<u64>();
        deserialize(&buf[offset..(offset + size_of::<u64>())]).map_err(Into::into)
    }
}

#[async_trait::async_trait]
impl FileIndexTrait for BPTreeFileIndex {
    /* from_file */
    /* from_records */
    /* file_size */
    /* records_count */
    /* read_meta */
    /* get_record_headers */
    /* validate */

    async fn find_by_key(&self, key: &[u8]) -> Result<Option<Vec<RecordHeader>>> {
        let root_offset = self.metadata.root_offset;
        let mut buf = [0u8; BLOCK_SIZE];
        let leaf_offset = self.find_leaf_node(key, root_offset, &mut buf).await?;
        if let Some((fh_offset, amount)) =
            self.find_first_header(leaf_offset, key, &mut buf).await?
        {
            let headers = self.read_headers(fh_offset, amount as usize).await?;
            Ok(Some(headers))
        } else {
            Ok(None)
        }
    }

    async fn get_any(&self, key: &[u8]) -> Result<Option<RecordHeader>> {
        let root_offset = self.metadata.root_offset;
        let mut buf = [0u8; BLOCK_SIZE];
        let leaf_offset = self.find_leaf_node(key, root_offset, &mut buf).await?;
        if let Some((first_header_offset, _amount)) =
            self.find_first_header(leaf_offset, key, &mut buf).await?
        {
            let header = self.read_headers(first_header_offset, 1).await?.remove(0);
            Ok(Some(header))
        } else {
            Ok(None)
        }
    }
}

impl BPTreeFileIndex {
    async fn find_leaf_node(&self, key: &[u8], mut offset: u64, buf: &mut [u8]) -> Result<u64> {
        while offset >= self.metadata.tree_offset {
            offset = if offset >= self.metadata.root_offset {
                Node::key_offset_serialized(&self.root_node, key)?
            } else {
                self.file.read_at(buf, offset).await?;
                Node::key_offset_serialized(buf, key)?
            };
        }
        Ok(offset)
    }

    async fn find_first_header(
        &self,
        leaf_offset: u64,
        key: &[u8],
        buf: &mut [u8],
    ) -> Result<Option<(u64, u64)>> {
        let leaf_size = key.len() + size_of::<u64>();
        let buf_size = self.leaf_node_buf_size(leaf_size, leaf_offset);
        let pointers_size = self.file.read_at(&mut buf[..buf_size], leaf_offset).await?;
        self.search_header_pointer(&buf[..pointers_size], key, buf_size as u64 + leaf_offset)
    }

    fn leaf_node_buf_size(&self, leaf_size: usize, leaf_offset: u64) -> usize {
        let buf_size = (self.metadata.tree_offset - leaf_offset) as usize;
        buf_size
            .min(BLOCK_SIZE - (BLOCK_SIZE % leaf_size))
            .min((self.metadata.tree_offset - leaf_offset) as usize)
    }

    fn search_header_pointer(
        &self,
        header_pointers: &[u8],
        key: &[u8],
        absolute_buf_end: u64,
    ) -> Result<Option<(u64, u64)>> {
        let leaf_size = key.len() + size_of::<u64>();
        let mut l = 0i32;
        let mut r: i32 = (header_pointers.len() / leaf_size) as i32 - 1;
        while l <= r {
            let m = (l + r) / 2;
            let m_off = leaf_size * m as usize;
            match key.cmp(&header_pointers[m_off..(m_off + key.len())]) {
                CmpOrdering::Less => r = m - 1,
                CmpOrdering::Greater => l = m + 1,
                CmpOrdering::Equal => {
                    return Ok(Some(self.with_amount(
                        header_pointers,
                        m as usize,
                        absolute_buf_end,
                        key.len(),
                    )?));
                }
            }
        }
        Ok(None)
    }

    fn with_amount(
        &self,
        header_pointers: &[u8],
        mid: usize,
        buf_end: u64,
        key_size: usize,
    ) -> Result<(u64, u64)> {
        let leaf_size = key_size + size_of::<u64>();
        let is_last_rec = (mid + 1) * leaf_size >= header_pointers.len()
            && (buf_end >= self.metadata.tree_offset);
        let mid_offset = mid * leaf_size + key_size;
        let cur_offset =
            deserialize::<u64>(&header_pointers[mid_offset..(mid_offset + size_of::<u64>())])?;
        let next_offset = if is_last_rec {
            self.metadata.leaves_offset
        } else {
            let next_offset = mid_offset + leaf_size;
            deserialize(&header_pointers[next_offset..(next_offset + size_of::<u64>())])?
        };
        let amount = (next_offset - cur_offset) / self.header.record_header_size as u64;
        Ok((cur_offset, amount))
    }

    async fn read_headers(&self, offset: u64, amount: usize) -> Result<Vec<RecordHeader>> {
        let mut buf = vec![0u8; self.header.record_header_size * amount];
        self.file.read_at(&mut buf, offset).await?;
        buf.chunks(self.header.record_header_size).try_fold(
            Vec::with_capacity(amount),
            |mut acc, bytes| {
                acc.push(deserialize(&bytes)?);
                Ok(acc)
            },
        )
    }

    async fn read_index_header(file: &File) -> Result<IndexHeader> {
        let header_size = IndexHeader::serialized_size_default()? as usize;
        let mut buf = vec![0; header_size];
        file.read_at(&mut buf, 0).await?;
        IndexHeader::from_raw(&buf).map_err(Into::into)
    }

    async fn read_root(file: &File, root_offset: u64) -> Result<[u8; BLOCK_SIZE]> {
        let mut buf = [0; BLOCK_SIZE];
        file.read_at(&mut buf, root_offset).await?;
        Ok(buf)
    }

    async fn read_tree_meta(file: &File, header: &IndexHeader) -> Result<TreeMeta> {
        let meta_size = TreeMeta::serialized_size_default()? as usize;
        let mut buf = vec![0; meta_size];
        let fsize = header.meta_size as u64;
        let hs = header.serialized_size()?;
        let meta_offset = hs + fsize + (header.records_count * header.record_header_size) as u64;
        file.read_at(&mut buf, meta_offset).await?;
        TreeMeta::from_raw(&buf).map_err(Into::into)
    }
    /* serialize */
    /* validate_header */
    /* hash_valid */
}
