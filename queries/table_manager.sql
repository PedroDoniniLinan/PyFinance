drop table general_schema.temp;

create table general_schema.temp (
	id int not null auto_increment,
    name varchar(255) not null,
    age int not null,
    email varchar(255) not null,
    birthday date not null,
    salary float not null,
    primary key (id)
);

insert into general_schema.temp (name, age, email) values
('Pedros', 26, 'test2@gmail.com');

select *
from general_schema.temp;

insert into general_schema.temp 
(id, name, age, email) values (2, 'Pedros', 26, 'test2@gmail.com') as src
on duplicate key update id = src.id; 

delete from general_schema.temp where id = 1; 

insert into general_schema.users 
select * from (select id, name, age, email from general_schema.temp) as src
on duplicate key update 
    name = src.name,
    age = src.age,
    email = src.email
; 

create table general_schema.temp (
	id int not null auto_increment,
    name varchar(255) not null,
    age int not null,
    email varchar(255) not null,
    birthday date not null,
    salary float not null,
    city_id int not null
    primary key (id),
    constraint fk_city
        foreign key (city_id) references general_schema.cities (city_id)
        on udpate cascade
        on delete restrict
);

update general_schema.temp set id = 5 where id = 2;
