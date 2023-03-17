create table pyfinance.balances (
    account varchar(255) not null,
    calendar_date date not null,
    currency varchar(255) not null,
    amount float,
    primary key (account, calendar_date, currency)
);