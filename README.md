
# db_account

Retrieve account information from your DB bank account


## Description

This tool will fetch the current balance and all account transactions from your DB (a big German bank) account, every time this script is executed. Transactions and balance are stored in a SQLite database in your home directory, also an email is sent to you with the current balance and all new transactions. Multiple schedules (frequently and infrequently used accounts) can be handled by creating multiple config files.

The bank in question allows you to send daily information about your current account balance, and every account movement greater 1€. However the email is not very helpful, as it contains no additional information, and even masks parts of the account number.


## Usage

Update the _account.yaml_ and fill in your bank account details. Multiple accounts or sub accounts can be specified.

Execute the script:

```
./account_statement.py -v -c account.yaml
```

Scheduling as a cron job for different accounts (daily, weekly):

```
30 5 * * 1-6 ./account_statement.py -q -c frequently_used_accounts.yaml
30 5 * * 6 ./account_statement.py -q -c infrequently_used_accounts.yaml
```
