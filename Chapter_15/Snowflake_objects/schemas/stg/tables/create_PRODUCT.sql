-- create tables in the STG schema, simulating tables populated from the source system using a data integration tool or custom solution
create or alter table STG.PRODUCT (
product_id integer,
product_name varchar,
category varchar,
min_quantity integer,
price number(18,2),
valid_from date
);

-- delete the data in case it already exists to avoid duplication
delete from STG.PRODUCT;

insert into STG.PRODUCT values
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