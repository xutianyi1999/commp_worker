use anyhow::Result;
use tokio::io::{AsyncRead, AsyncReadExt};

const MASK_SKIP_HIGH_2: u128 = 0b0011_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111;

macro_rules! process_fr {
    (
        $in_buffer:expr,
        $out0:expr,
        $out1:expr,
        $bit_offset:expr
    ) => {{
        $out0 = $in_buffer[0] >> 128 - $bit_offset;
        $out0 |= $in_buffer[1] << $bit_offset;
        $out1 = $in_buffer[1] >> 128 - $bit_offset;
        $out1 |= $in_buffer[2] << $bit_offset;
        $out1 &= MASK_SKIP_HIGH_2; // zero high 2 bits
    }};
}

#[inline(always)]
fn process_block(in_buffer: &[u8; 128], out_buffer: &mut [u8; 128]) {
    let (in_buffer, out_buffer): (&[u128; 8], &mut [u128; 8]) = unsafe {
        std::mem::transmute((in_buffer, out_buffer))
    };

    // 0..254
    {
        out_buffer[0] = in_buffer[0];
        out_buffer[1] = in_buffer[1] & MASK_SKIP_HIGH_2;
    }
    // 254..508
    process_fr!(&in_buffer[1..], out_buffer[2], out_buffer[3], 2);
    // 508..762
    process_fr!(&in_buffer[3..], out_buffer[4], out_buffer[5], 4);
    // 762..1016
    process_fr!(&in_buffer[5..], out_buffer[6], out_buffer[7], 6);
}

#[inline(always)]
pub async fn read_block<R: AsyncRead + Unpin>(reader: &mut R, out: &mut [u8; 128]) -> Result<bool> {
    let mut buff = [0u8; 128];
    reader.read_exact(&mut buff[..127]).await?;

    let mut is_zero = true;

    for x in buff {
        if x != 0 {
            is_zero = false;
            break;
        }
    }

    if !is_zero {
        process_block(&buff, out);
    }
    Ok(is_zero)
}