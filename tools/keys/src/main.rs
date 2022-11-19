use std::{fs::File, io::Read, path::Path};

use clap::{App, Arg, SubCommand};

fn main() -> anyhow::Result<()> {
    let matches = App::new("Conduit Key Wrangler")
        .subcommand(
            SubCommand::with_name("convert_synapse")
                .arg(
                    Arg::with_name("in")
                        .short("i")
                        .long("in")
                        .takes_value(true)
                        .required(true)
                        .long_help("The synapse key file to read from."),
                )
                .arg(
                    Arg::with_name("out")
                        .short("o")
                        .long("out")
                        .takes_value(true)
                        .required(true)
                        .long_help(
                            "The DER key file to output to. (Creates or overwrites(!) the file)",
                        ),
                ),
        )
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("convert_synapse") {
        let in_file = matches.value_of("in").expect("Was already required");
        let out_file = matches.value_of("out").expect("Was already required");

        let in_path = Path::new(in_file).canonicalize()?;
        let out_path = Path::new(out_file).canonicalize()?;

        if in_path.is_dir() {
            return Err(anyhow::anyhow!("input path is a directory"));
        } else if out_path.is_dir() {
            return Err(anyhow::anyhow!("output path exists, and is a directory"));
        }

        let mut in_file = File::open(in_path)?;
        let mut out_file = File::create(out_path)?;

        // 65K, arbitrary, don't load large files right off the bat
        if in_file.metadata()?.len() > (1 << 16) {
            return Err(anyhow::anyhow!(
                "input file is too large (>65KB) for a synapse signing key"
            ));
        }

        let mut data = Vec::new();

        in_file.read_to_end(&mut data)?;

        drop(in_file);
    }

    Ok(())
}
