;;; -*- mode: Lisp; Syntax: Common-Lisp; -*-
;;;
;;; Copyright (c) 2006 by the authors.
;;;
;;; See LICENCE for details.

(in-package :hu.dwim.rdbms.oracle)

;;;;;;
;;; Literals

(def method format-sql-literal ((literal vector) (database oracle))
  (format-string "to_Blob('")
  (loop for el across literal
        do (format *sql-stream* "~2,'0x" el))
  (format-string "')"))

(def method format-sql-literal ((value (eql nil)) (database oracle))
  (format-string "'F'"))

(def method format-sql-literal ((value (eql t)) (database oracle))
  (format-string "'T'"))

(def method format-sql-literal ((literal sql-literal) (database oracle))
  (if (unquote-aware-format-sql-literal literal)
      (progn
        (format-string ":")
        (format-string (princ-to-string (length *binding-types*))))
      (call-next-method)))

;;;;;;
;;; Bindings

(def method format-sql-syntax-node ((variable sql-binding-variable) (database oracle))
  (unquote-aware-format-sql-binding-variable variable)
  (format-string ":")
  (format-string (princ-to-string (length *binding-types*))))

;;;;;;
;;; Types

(def method format-sql-syntax-node ((self sql-boolean-type) (database oracle))
  (format-string "CHAR(1)"))

(def method format-sql-syntax-node ((self sql-character-type) (database oracle))
  ;; signal an error when char(1) type is used
  ;; because it would be interpreted as boolean and 'T' and 'F' would be mapped to t/nil
  (with-slots (size) self
    (if (and size (= size 1))
        (error "CHAR(1) is reserved for booleans in Oracle mapping")
        (progn
          (format-string "CHAR")
          (format-character-size size)))))

(def method format-sql-syntax-node ((self sql-float-type) (database oracle))
  (with-slots (bit-size) self
    (assert (and bit-size (<= 32 bit-size 64)))
    (cond
      ((<= bit-size 32) (format-string "BINARY_FLOAT"))
      ((<= bit-size 64) (format-string "BINARY_DOUBLE")))))

(def method format-sql-syntax-node ((self sql-integer-type) (database oracle))
  (with-slots (bit-size) self
    (cond
      ((cl:null bit-size) (format-string "NUMBER"))
      ((<= bit-size 16) (format-string "NUMBER(5)"))
      ((<= bit-size 32) (format-string "NUMBER(10)"))
      ((<= bit-size 64) (format-string "NUMBER(19)"))
      (t (format-string "NUMBER")))))

(def method format-sql-syntax-node ((self sql-character-varying-type) (database oracle))
  (with-slots (size) self
    (format-string "VARCHAR2")
    (format-character-size size)))

(def method format-sql-syntax-node ((self sql-character-large-object-type) (database oracle))
  (format-string "CLOB")) ; size ignored

(def method format-sql-syntax-node ((self sql-time-type) (database oracle))
  (format-string "TIMESTAMP"))

(def method format-sql-syntax-node ((self sql-interval-type) (database oracle))
  (error "sql-interval-type not yet supported"))

(def method format-sql-syntax-node ((self sql-binary-large-object-type) (database oracle))
  (aif (size-of self)
       (progn
         (format-string "RAW(")
         (format-size it)
         (format-string "RAW)"))
       (format-string "BLOB")))

(def function format-character-size (size)
  (when size
    (format-string "(")
    (format-number size)
    (format-string " CHAR)")))

;;;;;;
;;; Identifiers

(def method format-sql-identifier :around ((identifier string) (database oracle))
  (progn
    (write-char #\" *sql-stream*)
    (call-next-method)
    (write-char #\" *sql-stream*)))

(def special-variable *oracle-sql-reserved-words* (make-hash-table :test 'equal))

(eval-when (:load-toplevel)
  (mapc (lambda (word) (setf (gethash word *oracle-sql-reserved-words*) #t))
        '("ACCESS" "ADD" "ALL" "ALTER" "AND" "ANY" "AS" "ASC" "AUDIT" "BETWEEN" "BY" 
          "CHAR" "CHECK" "CLUSTER" "COLUMN" "COMMENT" "COMPRESS" "CONNECT" "CREATE" 
          "CURRENT" "DATE" "DECIMAL" "DEFAULT" "DELETE" "DESC" "DISTINCT" "DROP" 
          "ELSE" "EXCLUSIVE" "EXISTS" "FILE" "FLOAT" "FOR" "FROM" "GRANT" "GROUP" 
          "HAVING" "IDENTIFIED" "IMMEDIATE" "IN" "INCREMENT" "INDEX" "INITIAL" "INSERT" 
          "INTEGER" "INTERSECT" "INTO" "IS" "LEVEL" "LIKE" "LOCK" "LONG" "MAXEXTENTS" 
          "MINUS" "MLSLABEL" "MODE" "MODIFY" "NOAUDIT" "NOCOMPRESS" "NOT" "NOWAIT" 
          "NULL" "NUMBER" "OF" "OFFLINE" "ON" "ONLINE" "OPTION" "OR" "ORDER" "PCTFREE" 
          "PRIOR" "PRIVILEGES" "PUBLIC" "RAW" "RENAME" "RESOURCE" "REVOKE" "ROW" "ROWID" 
          "ROWNUM" "ROWS" "SELECT" "SESSION" "SET" "SHARE" "SIZE" "SMALLINT" "START" 
          "SUCCESSFUL" "SYNONYM" "SYSDATE" "TABLE" "THEN" "TO" "TRIGGER" "UID" "UNION" 
          "UNIQUE" "UPDATE" "USER" "VALIDATE" "VALUES" "VARCHAR" "VARCHAR2" "VIEW" "WHENEVER" 
          "WHERE" "WITH")))

(def function reserved-word-p (word)
  (gethash (string-upcase word) *oracle-sql-reserved-words*))

;;;;;;
;;; Sequences

(def method format-sql-syntax-node ((self sql-sequence-nextval-column) (database oracle))
  (with-slots (name) self
    (format-sql-identifier name database)
    (format-string ".nextval")))

;;;;;;
;;; Selects
;;; add FROM dual when no table, ignore OFFSET, LIMIT

(def method format-sql-syntax-node ((self sql-select) (database oracle))
  (with-slots (distinct columns tables where order-by offset limit for wait) self
    (format-string "SELECT ")
    (when distinct
      (format-string "DISTINCT "))
    (format-comma-separated-list columns database 'format-sql-column-reference)
    (if tables
        (progn
          (format-string " FROM ")
          (format-comma-separated-list tables database 'format-sql-table-reference))
        (format-string " FROM dual "))
    (format-sql-where where database)
    (when order-by
      (format-string " ORDER BY ")
      (format-comma-separated-list order-by database))
    (when for
      (format-string " FOR ")
      (format-string (symbol-name for))
      (unless wait
        (format-string " NOWAIT")))))

(def function format-sql-column-reference (column database)
  (typecase column
    ((or symbol string sql-column sql-column-alias) (format-sql-identifier column database))
    (t (format-sql-syntax-node column database))))

;;;;;;
;;; Expressions

(def method format-sql-syntax-node ((regexp-like sql-regexp-like) (database oracle))
  (format-string "REGEXP_LIKE(")
  (format-sql-syntax-node (string-of regexp-like) database)
  (format-string ", ")
  (format-sql-syntax-node (pattern-of regexp-like) database)
  (format-string ", ")
  (format-string (if (case-sensitive-p regexp-like) "'c'" "'i'"))
  (format-char ")"))

#+nil
(def method format-sql-syntax-node ((self sql-case) (database oracle))
  (with-slots (clauses) self
    (if (or (/= (length clauses) 2)
            (not (eq (first (second clauses)) t)))
        (call-next-method)
        (let ((cond (first (first clauses)))
              (then (second (first clauses)))
              (else (second (second clauses))))
          (progn
            (format-string "sql_if(")
            (format-sql-syntax-node cond database)
            (format-char ",")
            (format-sql-syntax-node then database)
            (format-char ",")
            (format-sql-syntax-node else database)
            (format-string ")"))))))
