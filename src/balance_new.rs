use clap::Parser;
use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::{EncodableKey, Signer},
    transaction::Transaction,
};
use solana_transaction_status::UiTransactionEncoding;
use tracing::{error, info};

use crate::{constant, Miner};

#[derive(Parser, Debug, Clone)]
pub struct BatchTransferArgs {
    #[arg(long)]
    pub fee_payer: String, // 新增指定交易费支付者参数

    #[arg(long)]
    pub max_value: f64,

    #[arg(long = "address", value_delimiter = ',')]
    pub addresses: Vec<Pubkey>,
}

impl Miner {
    pub async fn batch_transfer(&self, args: &BatchTransferArgs) {
        let client = Self::get_client_confirmed(&self.rpc);

        let fee_payer = Keypair::read_from_file(&args.fee_payer).unwrap(); // 读取指定的费用支付者的密钥对

        let balance = client.get_balance(&fee_payer.pubkey()).await.unwrap(); // 获取费用支付者的余额
        info!("fee payer: {}, balance: {}", fee_payer.pubkey(), balance);

        info!("accounts to distribute: {}", args.addresses.len());

        let max_lamports = spl_token::ui_amount_to_amount(args.max_value, 9);
        let transaction_fee = 5000; // 假设交易费为 5000 lamports

        let mut amount_to_filled: Vec<(Pubkey, u64)> = vec![];

        for batch in args.addresses.chunks(constant::FETCH_ACCOUNT_LIMIT) {
            let account_data = client.get_multiple_accounts(batch).await.unwrap();
            info!(batch_size = batch.len(), "fetched accounts");

            for (address, account) in batch.iter().zip(account_data.iter()) {
                let available_balance = if let Some(acc) = account {
                    acc.lamports.saturating_sub(transaction_fee) // 扣除交易费后的可用余额
                } else {
                    max_lamports
                };

                let amount = if available_balance > max_lamports {
                    max_lamports
                } else {
                    available_balance
                };

                if amount > 0 {
                    amount_to_filled.push((*address, amount));
                }
            }
        }

        if amount_to_filled.is_empty() {
            info!("no account to fill");
            return;
        }

        let total_amount = amount_to_filled.iter().map(|(_, amount)| amount).sum::<u64>();

        info!(
            "total amount to transfer: {}",
            spl_token::amount_to_ui_amount(total_amount, 9)
        );

        // 根据需要调整批处理大小
        let batch_size = 5; // 每个批次包含的交易数量
        let mut batch_and_txs = amount_to_filled
            .chunks(batch_size)
            .map(|batch| (batch.to_vec(), Signature::default()))
            .collect::<Vec<_>>();

        while !batch_and_txs.is_empty() {
            let (slot, blockhash) = match Self::get_latest_blockhash_and_slot(&client).await {
                Ok(r) => r,
                Err(err) => {
                    error!("failed to get latest blockhash: {:#}", err);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            for (batch, sig) in batch_and_txs.iter_mut() {
                let mut addresses = vec![];
                let instructions = batch
                    .iter()
                    .map(|(address, amount)| {
                        addresses.push(address.to_string());
                        solana_sdk::system_instruction::transfer(&fee_payer.pubkey(), address, *amount)
                    })
                    .collect::<Vec<_>>();

                let tx =
                    Transaction::new_signed_with_payer(&instructions, Some(&fee_payer.pubkey()), &[&fee_payer], blockhash);

                let calculated_sig = tx.signatures.first().unwrap();
                *sig = *calculated_sig;

                let send_cfg = RpcSendTransactionConfig {
                    skip_preflight: false,
                    preflight_commitment: Some(CommitmentLevel::Confirmed),
                    encoding: Some(UiTransactionEncoding::Base58),
                    max_retries: Some(5),
                    min_context_slot: Some(slot),
                };

                let send_result = client.send_transaction_with_config(&tx, send_cfg).await;
                let total_amount = batch.iter().map(|(_, amount)| amount).sum::<u64>();

                match send_result {
                    Ok(sig) => info!(
                        "transaction sent: {sig}, amount: {}, addresses: {addresses:?}",
                        spl_token::amount_to_ui_amount(total_amount, 9)
                    ),
                    Err(err) => error!(tx = %calculated_sig, "failed to send tx: {err:#}"),
                }
            }

            let mut latest_slot = slot;
            let mut signatures = batch_and_txs.iter().map(|(_, sig)| *sig).collect::<Vec<_>>();

            while !signatures.is_empty() && latest_slot <= slot + constant::SLOT_EXPIRATION {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                info!(
                    remaining_tx = signatures.len(),
                    "waiting for all transactions to be confirmed"
                );

                let response = match client.get_signature_statuses(&signatures).await {
                    Ok(r) => r,
                    Err(err) => {
                        error!("failed to get signature statuses: {:#}", err);
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        continue;
                    }
                };

                latest_slot = response.context.slot;
                let statuses = response.value;

                let mut sig_to_purge_in_query = vec![];

                for (status, sig) in statuses.iter().zip(signatures.iter()) {
                    let status = match status {
                        None => continue,
                        Some(s) => s,
                    };

                    if !status.satisfies_commitment(CommitmentConfig::confirmed()) {
                        continue;
                    }

                    sig_to_purge_in_query.push(*sig);

                    match &status.err {
                        None => {
                            info!(tx = %sig, "transaction confirmed: {sig}");
                            batch_and_txs.retain(|(_, s)| !s.eq(sig));
                        }
                        Some(err) => {
                            error!(tx = %sig, "transaction failed: {err:#}");
                        }
                    }
                }

                signatures.retain(|s| !sig_to_purge_in_query.contains(s));
            }
        }
    }
}
