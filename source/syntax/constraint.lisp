(in-package :hu.dwim.rdbms)

(def syntax-node sql-constraint (named-sql-syntax-node)
  ()
  (:documentation "An SQL constraint."))

(def syntax-node sql-constraint-with-tablespace (sql-constraint)
  ((tablespace
    nil
    :type string)))

(def syntax-node sql-primary-key-constraint (sql-constraint-with-tablespace)
  ()
  (:format-sql-syntax-node
   (format-string " PRIMARY KEY")))

(def syntax-node sql-null-constraint (sql-constraint)
  ()
  (:format-sql-syntax-node
   (format-string " NULL")))

(def syntax-node sql-not-null-constraint (sql-constraint)
  ()
  (:format-sql-syntax-node
   (format-string " NOT NULL")))

(def syntax-node sql-unique-constraint (sql-constraint-with-tablespace)
  ()
  (:format-sql-syntax-node
   (format-string " UNIQUE")))

(def method format-sql-syntax-node :before ((constraint sql-constraint) database)
  (awhen (name-of constraint)
    (format-string " CONSTRAINT")
    (format-char " ")
    (format-sql-syntax-node it database)))
