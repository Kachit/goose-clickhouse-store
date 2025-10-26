CREATE TABLE employee (
    emp_no  UInt32 NOT NULL,
    birth_date  Date NOT NULL,
    first_name  String NOT NULL,
    last_name  String NOT NULL,
    gender     Enum8('M', 'F' ) NOT NULL,
    hire_date Date NOT NULL
) ENGINE = MergeTree() PRIMARY KEY (emp_no);