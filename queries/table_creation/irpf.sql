create table pyfinance.irpf (
    id int not null,
    ticker varchar(255) not null,
    calendar_date date not null,
    avg_price float not null,
    units float not null,
    position float not null
);