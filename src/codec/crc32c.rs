const POLYNOMIAL: u32 = 0x82f6_3b78;
const TABLE: [u32; 256] = table();

const fn table() -> [u32; 256] {
    let mut table = [0; 256];
    let mut index = 0;
    while index < table.len() {
        let mut crc = index as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = (crc >> 1) ^ if crc & 1 == 1 { POLYNOMIAL } else { 0 };
            bit += 1;
        }
        table[index] = crc;
        index += 1;
    }
    table
}

#[derive(Clone, Copy, Debug)]
pub struct Crc32c(u32);

impl Default for Crc32c {
    fn default() -> Self {
        Self(!0)
    }
}

impl Crc32c {
    pub fn update(&mut self, bytes: &[u8]) {
        let mut crc = self.0;
        for &byte in bytes {
            crc = (crc >> 8) ^ TABLE[((crc as u8) ^ byte) as usize];
        }
        self.0 = crc;
    }

    pub fn finish(self) -> u32 {
        !self.0
    }
}

pub fn checksum(bytes: &[u8]) -> u32 {
    let mut crc = Crc32c::default();
    crc.update(bytes);
    crc.finish()
}
