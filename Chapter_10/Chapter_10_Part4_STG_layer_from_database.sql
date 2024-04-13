use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema STG;

-- create tables in the STG schema, simulating tables populated from the source system using a data integration tool or custom solution
create table PARTNERS (
partner_id integer,
partner_name varchar,
address varchar,
rating varchar,
valid_from date
);

insert into PARTNERS values
(101, 'Coffee Pocket', '501 Courtney Wells', 'A', '2023-06-01'),
(102, 'Lily''s Coffee', '2825 Joshua Forest', 'A', '2023-06-01'),
(103, 'Crave Coffee', '538 Hayden Port', 'B', '2023-06-01'),
(104, 'Best Burgers', '790 Friedman Valley', 'A', '2023-06-01'),
(105, 'Page One Fast Food', '44864 Amber Walk', 'B', '2023-06-01'),
(106, 'Jimmy''s Diner', '2613 Scott Mountains', 'A', '2023-06-01'),
(107, 'Metro Fine Foods', '520 Castillo Valley', 'A', '2023-06-01'),
(108, 'New Bistro', '494 Terry Spurs', 'A', '2023-06-01'),
(109, 'Park Inn', '3692 Nelson Turnpike', 'A', '2023-06-01'),
(110, 'Chef Supplies', '870 Anthony Hill', 'A', '2023-06-01'),
(111, 'Farm Fresh', '23633 Melanie Ranch', 'A', '2023-06-01'),
(112, 'Murphy Mill', '700 Darren Centers', 'A', '2023-06-01');

select * from PARTNERS;

create table PRODUCTS (
product_id integer,
product_name varchar,
category varchar,
min_quantity integer,
price number(18,2),
valid_from date
);

insert into PRODUCTS values
(1, 'Baguette', 'Bread', 2, 2.5, '2023-06-01'),
(2, 'Bagel', 'Bread', 6, 1.3, '2023-06-01'), 
(3, 'English Muffin', 'Bread', 6, 1.2, '2023-06-01'), 
(4, 'Croissant', 'Pastry', 4, 2.1, '2023-06-01'), 
(5, 'White Loaf', 'Bread', 1, 1.8, '2023-06-01'), 
(6, 'Hamburger Bun', 'Bread', 10, 0.9, '2023-06-01'), 
(7, 'Rye Loaf', 'Bread', 1, 3.2, '2023-06-01'), 
(8, 'Whole Wheat Loaf', 'Bread', 1, 2.8, '2023-06-01'), 
(9, 'Muffin', 'Pastry', 12, 3.0, '2023-06-01'), 
(10, 'Cinnamon Bun', 'Pastry', 6, 3.4, '2023-06-01'), 
(11, 'Blueberry Muffin', 'Pastry', 12, 3.6, '2023-06-01'), 
(12, 'Chocolate Muffin', 'Pastry', 12, 3.6, '2023-06-01'); 

select * from PRODUCTS;
