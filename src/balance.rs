use std::{collections::HashMap, path::Path};

use alloy::primitives::{Address, U256, U512, aliases::I512};
use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufWriter},
};

use crate::TransferEvent;

#[derive(Default)]
pub struct BalanceSheet {
    // (token, account) -> (inflow, outflow)
    pub in_out_flow: HashMap<(Address, Address), (U256, U256)>,
    pub total_transfers: usize,
}

impl BalanceSheet {
    pub fn extend(&mut self, events: impl Iterator<Item = TransferEvent>) {
        for event in events {
            self.in_out_flow
                .entry((event.token, event.to))
                .and_modify(|(inflow, _)| *inflow += event.value)
                .or_insert((event.value, U256::ZERO));

            self.in_out_flow
                .entry((event.token, event.from))
                .and_modify(|(_, outflow)| *outflow += event.value)
                .or_insert((U256::ZERO, event.value));

            self.total_transfers += 1;
        }
    }

    fn finish(self) -> HashMap<Address, HashMap<Address, I512>> {
        let mut balances: HashMap<Address, HashMap<Address, I512>> = HashMap::new();

        for ((token, account), (inflow, outflow)) in self.in_out_flow {
            let inflow = I512::from_raw(U512::from(inflow));
            let outflow = I512::from_raw(U512::from(outflow));
            let balance = inflow.saturating_sub(outflow);

            if balance.is_zero() {
                continue;
            }

            balances.entry(token).or_default().insert(account, balance);
        }

        balances
    }

    pub async fn finalize(self, output: &Path, from: u64, to: u64) -> eyre::Result<()> {
        let balances = self.finish();

        for (token, accounts) in balances {
            let csv = output.join(format!("{token}_{from}_{to}.csv"));
            let file = File::create(csv).await?;
            let mut writer = BufWriter::new(file);

            writer.write_all(b"account,balance\n").await?;

            for (account, balance) in accounts {
                writer
                    .write_all(format!("{account},{balance}\n").as_bytes())
                    .await?;
            }
        }

        Ok(())
    }
}
