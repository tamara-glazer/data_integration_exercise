create table sfinspection (
  business_name             varchar(100)    ,
  business_address          varchar(100)    ,
  business_city             varchar(100)    ,
  business_state            varchar(100)    ,
  business_postal_code      varchar(100)    ,
  business_latitude         float8   ,
  business_longitude        float8    ,
  business_phone_number     varchar(20)    ,
  inspection_id             varchar(100)    ,
  inspection_date           date    ,
  inspection_score          real,
  inspection_type           varchar(100)    ,
  violation_id              varchar(100)    ,
  violation_description     varchar(100)    ,
  risk_category             varchar(100)    );


create table cleanrest (
    business_name             varchar(100)    ,
    business_address          varchar(100)    ,
    business_city             varchar(100)    ,
    business_state            varchar(100)    ,
    business_postal_code      varchar(100)    ,
    business_latitude         float8   ,
    business_longitude        float8    ,
    business_phone_number     varchar(20) ,
    primary key (business_name,business_address)
);

create table cleaninspection (
  business_name             varchar(100)    ,
  business_address          varchar(100)    ,
  inspection_id             varchar(100)    ,
  inspection_date           date    ,
  inspection_score          real ,
  inspection_type           varchar(100)    ,
  violation_id              varchar(100)    ,
  violation_description     varchar(100)    ,
  risk_category             varchar(100)   ,
  foreign key (business_name,business_address) references cleanrest
  );

create table joinedinspbike (
  duration int,
  bike_id int,
  violation_id              varchar(100)    ,
  inspection_date           date    
);
