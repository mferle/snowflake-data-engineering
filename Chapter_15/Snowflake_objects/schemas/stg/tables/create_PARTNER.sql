-- create tables in the STG schema, simulating tables populated from the source system using a data integration tool or custom solution
create or alter table STG.PARTNER (
partner_id integer,
partner_name varchar,
address varchar,
rating varchar,
valid_from date
);

-- delete the data in case it already exists to avoid duplication
delete from STG.PARTNER;

insert into STG.PARTNER values
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
