create table pyfinance.sales (
    tag varchar(255) not null,
    security_type varchar(255) not null,
    ticker varchar(255) not null,
    currency varchar(255) not null,
    calendar_date date not null,
    units float not null,
    avg_price float not null,
    sale_amount float not null,
    sale_price float not null,
    profit float not null
);