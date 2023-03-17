create table pyfinance.transfers (
    id int not null auto_increment,
    source_acc varchar(255) not null,
    destination_acc varchar(255) not null,
    calendar_date date not null,
    amount float not null,
    currency varchar(255) not null,
    count_to_balance boolean not null,
    primary key (id)
);
