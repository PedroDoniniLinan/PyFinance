create table pyfinance.transactions (
    id int not null auto_increment,
    tag varchar(511) not null,
    amount float not null,
    account varchar(255) not null,
    calendar_date date not null,
    category varchar(255) not null,
    subcategory varchar(255) not null,
    currency varchar(255) not null,
    count_to_balance boolean not null,
    transaction_type varchar(255) not null,
    primary key (id)
);

-- insert into pyfinance.transactions (tag, amount, calendar_date, account, category, subcategory, count_to_balance, currency) values
-- ('Grana inicial', 12508.53, '2019-08-01', 'BB', 'Parents', 'Support', False, 'BRL');


-- insert into general_schema.temp (tag, amount, calendar_date, account, category, subcategory, count_to_balance, currency) 
-- values ('Grana inicial', 12508.53, '2019-08-01', 'BB', 'Parents', 'Support', False, 'BRL') as src
-- on duplicate key update 
-- tag = src.tag,
-- calendar_date = src.calendar_date,
-- account = src.account,
-- currency = src.currency
-- ; 