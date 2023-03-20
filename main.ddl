create table de10.kvrk_stg_transactions(
    trans_id varchar(50),
    trans_date varchar(25),
    amt varchar(10),
    card_num varchar(50),
    oper_type varchar(25),
    oper_result varchar(50),
    terminal varchar(25)
);



create table de10.kvrk_dwh_fact_transactions(
    trans_id varchar(50),
    trans_date timestamp(0),
    amt numeric(8,3),
    card_num varchar(20),
    oper_type varchar(25),
    oper_result varchar(50),
    terminal varchar(25)
);


create table de10.kvrk_stg_terminals(
    terminal_id varchar(25),
    terminal_type varchar(15),
    terminal_city varchar(25),
    terminal_address varchar(50),
    create_dt timestamp(0),
    update_dt timestamp(0) 
);


create table de10.kvrk_dwh_dim_terminals(
    terminal_id varchar(25),
    terminal_type varchar(15),
    terminal_city varchar(25),
    terminal_address varchar(50),
    create_dt timestamp(0),
    update_dt timestamp(0) 
);


create table de10.kvrk_stg_blacklist(
    entry_dt date,
    passport_num varchar(20)
);

create table de10.kvrk_dwh_fact_passport_blacklist(
    passport_num varchar(11),
    entry_dt date
);

create table de10.kvrk_stg_cards(
    card_num bpchar(20),
    account_num varchar(30),
    create_dt timestamp(0),
    update_dt timestamp(0)
);

create table de10.kvrk_dwh_dim_cards(
    card_num bpchar(20),
    account_num varchar(30),
    create_dt timestamp(0),
    update_dt timestamp(0)
);

create table de10.kvrk_stg_accounts(
    account_num varchar(50),
    valid_to date,
    client varchar(25),
    create_dt timestamp(0),
    update_dt timestamp(0)
);

create table de10.kvrk_dwh_dim_accounts(
    account_num varchar(50),
    valid_to date,
    client varchar(25),
    create_dt timestamp(0),
    update_dt timestamp(0)
);

create table de10.kvrk_stg_clients(
    client_id varchar(50),
    last_name varchar(50),
    first_name varchar(50),
    patronymic varchar(50),
    date_of_birth date,
    passport_num varchar(11),
    passport_valid_to date,
    phone varchar(20),
    create_dt timestamp(0),
    update_dt timestamp(0)
);

create table de10.kvrk_dwh_dim_clients(
    client_id varchar(50),
    last_name varchar(50),
    first_name varchar(50),
    patronymic varchar(50),
    date_of_birth date,
    passport_num varchar(11),
    passport_valid_to date,
    phone varchar(20),
    create_dt timestamp(0),
    update_dt timestamp(0)
);

create table de10.kvrk_rep_fraud(
    event_dt timestamp(0),
    passport varchar(25),
    fio varchar(75),
    phone varchar(20),
    event_type varchar(5),
    report_dt date
);