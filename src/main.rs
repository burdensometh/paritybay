#![forbid(unsafe_code)]

mod cli;

use std::io;
use std::process::ExitCode;

fn main() -> ExitCode {
    let stdout = io::stdout();
    let stderr = io::stderr();
    let mut out = stdout.lock();
    let mut err = stderr.lock();
    match cli::run(std::env::args_os().skip(1), &mut out, &mut err) {
        Ok(code) => ExitCode::from(code),
        Err(error) => {
            use io::Write;
            let _ = writeln!(err, "error={} {error}", error.code());
            ExitCode::from(match error.code() {
                "invalid" => 2,
                "missing" => 3,
                "conflict" => 4,
                "capacity" => 5,
                "unrecoverable" => 6,
                "corrupt" => 7,
                _ => 1,
            })
        }
    }
}
