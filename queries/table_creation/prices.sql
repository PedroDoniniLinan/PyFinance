create table pyfinance.prices (
    ticker varchar(255) not null,
    currency varchar(255) not null,
    calendar_date date not null,
    price float not null,
    primary key (ticker, currency, calendar_date)
);
