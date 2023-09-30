// pub async fn do_chunk() {
//     let cdc = cdc::make_stream(source);
// }

pub mod cdc {
    use fastcdc::v2020::AsyncStreamCDC;
    use tokio::io::AsyncRead;

    const MEG: u32 = 1024 * 1024;
    #[allow(clippy::identity_op)]
    const MIN_CHUNK: u32 = 1 * MEG;
    const AVG_CHUNK: u32 = 2 * MEG;
    const MAX_CHUNK: u32 = 4 * MEG;

    pub fn make_stream<R: AsyncRead + Unpin>(source: R) -> AsyncStreamCDC<R> {
        AsyncStreamCDC::new(source, MIN_CHUNK, AVG_CHUNK, MAX_CHUNK)
    }
}
