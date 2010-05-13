;;; -*- mode: Lisp; Syntax: Common-Lisp; -*-
;;;
;;; Copyright (c) 2006 by the authors.
;;;
;;; See LICENCE for details.

(in-package :hu.dwim.rdbms.oracle)

;;;;;;
;;; Literals

(def method format-sql-literal ((literal vector) (database oracle))
  (vector-push-extend nil *binding-variables*)
  (vector-push-extend (sql-binary-large-object-type) *binding-types*)
  (vector-push-extend literal *binding-values*)
  (format-string ":")
  (format-string (princ-to-string (length *binding-types*))))

(def method format-sql-literal ((value (eql nil)) (database oracle))
  (format-string "'N'"))

(def method format-sql-literal ((value (eql t)) (database oracle))
  (format-string "'Y'"))

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
  ;; because it would be interpreted as boolean and 'Y' and 'N' would be mapped to t/nil
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
      ((cl:null bit-size) (format-string "NUMBER(*,0)"))
      ((<= bit-size 16) (format-string "NUMBER(5,0)"))
      ((<= bit-size 32) (format-string "NUMBER(10,0)"))
      ((<= bit-size 64) (format-string "NUMBER(19,0)"))
      (t (format-string "NUMBER(*,0)")))))

(def method format-sql-syntax-node ((self sql-numeric-type) (database oracle))
  (error "use more specific type with oracle backend") ;; TODO THL handle this better?
  (format-string "NUMBER")) ;; oracle assumes NUMBER(*.0) :-{

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
  (format-string "BLOB"))

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

(def method format-sql-syntax-node ((self sql-select) (database oracle))
  (with-slots
        (distinct columns tables where group-by having order-by offset limit for wait)
      self
    (flet ((core ()
             (format-string "SELECT ")
             (when distinct
               (format-string "DISTINCT "))
             (format-comma-separated-list columns database 'format-sql-column-reference)
             (if tables
                 (progn
                   (format-string " FROM ")
                   (format-comma-separated-list tables database
                                                'format-sql-table-reference))
                 (format-string " FROM dual "))
             (format-sql-where where database)
             (when group-by
               (format-string " GROUP BY ")
               (format-comma-separated-list group-by database))
             (when having
               (format-string " HAVING ")
               (format-sql-syntax-node having database))
             (when order-by
               (format-string " ORDER BY ")
               (format-comma-separated-list order-by database))
             (when for
               (format-string " FOR ")
               (format-string (symbol-name for))
               (unless wait
                 (format-string " NOWAIT")))))
      (cond
        ((and (not limit) (not offset))
         (core))
        ((and limit (not offset))
         (format-string "SELECT * FROM (")
         (core)
         (format-string ") WHERE ROWNUM <= ")
         (format-sql-syntax-node limit database))
        (t
         (flet ((cols ()
                  (format-comma-separated-list
                   columns database
                   (lambda (x db) (format-sql-identifier (column-of x) db)))))
           (format-string "SELECT ")
           (cols)
           (format-string " FROM (SELECT ")
           (cols)
           (format-string ", ROWNUM \"kaeD8Ot7\" FROM (") ;; name unlikely to clash
           (core)
           (format-string ")) WHERE ")
           (format-sql-syntax-node offset database)
           (format-string " < \"kaeD8Ot7\"")
           (when limit
             (format-string " AND \"kaeD8Ot7\" <= (")
             (format-sql-syntax-node offset database)
             (format-string " + ")
             (format-sql-syntax-node limit database)
             (format-string ")"))))))))

(def function format-sql-column-reference (column database)
  (typecase column
    ((or symbol string sql-column sql-column-alias) (format-sql-identifier column database))
    (t (format-sql-syntax-node column database))))

(def syntax-node sql-empty-clob (sql-syntax-node)
  ()
  (:format-sql-syntax-node
   (format-string "EMPTY_CLOB()")))

(def syntax-node sql-empty-blob (sql-syntax-node)
  ()
  (:format-sql-syntax-node
   (format-string "EMPTY_BLOB()")))

(def method format-sql-syntax-node ((self sql-insert) (database oracle))
  (with-slots (table columns values subselect) self
    (flet ((make-lob (column)
             (etypecase (type-of column)
               (sql-character-large-object-type (make-instance 'sql-empty-clob))
               (sql-binary-large-object-type (make-instance 'sql-empty-blob)))))
      (let ((lob-columns nil)
            (other-columns nil)
            (lob-values nil)
            (other-values nil))
        (loop
           for column in columns
           for value in (when (slot-boundp self 'values) values)
           do (if (lobp (and (typep column 'sql-column) (type-of column)))
                  (progn
                    (push column lob-columns)
                    (push value lob-values))
                  (progn
                    (push column other-columns)
                    (push value other-values))))
        (format-string "INSERT INTO ")
        (format-sql-identifier table database)
        (let ((cols (append other-columns lob-columns)))
          (when cols
            (format-string " (")
            (format-comma-separated-identifiers cols database)
            (format-char ")")))
        (when (slot-boundp self 'values)
          (format-string " VALUES (")
          (format-comma-separated-list
           (append other-values (mapcar #'make-lob lob-columns))
           database)
          (format-char ")"))
        (when (slot-boundp self 'subselect)
          (format-sql-syntax-node subselect database))
        (when (and lob-columns
                   (remove-if (lambda (x) (member x '(:null nil))) lob-values))
          (format-string " RETURNING ")
          (loop
             for n = (length lob-columns)
             for column in lob-columns
             for value in lob-values
             for j from 1
             unless (member value '(:null nil))
             do (progn
                  (when (<= 2 j n)
                    (format-char ","))
                  (format-sql-identifier column database)))
          (format-string " INTO ")
          (loop
             for n = (length lob-columns)
             for column in lob-columns
             for value in lob-values
             for j from 1
             unless (member value '(:null nil))
             do (progn
                  (when (<= 2 j n)
                    (format-char ","))
                  (format-sql-literal value database))))))))

(def method format-sql-syntax-node ((self sql-update) (database oracle))
  (with-slots (table columns values where) self
    (flet ((make-lob (column)
             (etypecase (type-of column)
               (sql-character-large-object-type (make-instance 'sql-empty-clob))
               (sql-binary-large-object-type (make-instance 'sql-empty-blob)))))
      (let ((lob-columns nil)
            (other-columns nil)
            (lob-values nil)
            (other-values nil))
        (loop
           for column in columns
           for value in (when (slot-boundp self 'values) values)
           do (if (lobp (and (typep column 'sql-column) (type-of column)))
                  (progn
                    (push column lob-columns)
                    (push value lob-values))
                  (progn
                    (push column other-columns)
                    (push value other-values))))
        (format-string "UPDATE ")
        (format-sql-identifier table database)
        (format-string " SET ")
        (loop
           for column in (append other-columns lob-columns)
           for value in (append other-values (mapcar #'make-lob lob-columns))
           for n from 0
           do (progn
                (when (plusp n)
                  (format-string ", "))
                (format-sql-identifier column database)
                (format-string " = ")
                (format-sql-syntax-node value database)))
        (format-sql-where where database)
        (when (and lob-columns
                   (remove-if (lambda (x) (member x '(:null nil))) lob-values))
          (format-string " RETURNING ")
          (loop
             for n = (length lob-columns)
             for column in lob-columns
             for value in lob-values
             for j from 1
             unless (member value '(:null nil))
             do (progn
                  (when (<= 2 j n)
                    (format-char ","))
                  (format-sql-identifier column database)))
          (format-string " INTO ")
          (loop
             for n = (length lob-columns)
             for column in lob-columns
             for value in lob-values
             for j from 1
             unless (member value '(:null nil))
             do (progn
                  (when (<= 2 j n)
                    (format-char ","))
                  (format-sql-literal value database))))))))

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

(def method format-sql-syntax-node ((self sql-case) (database oracle))
  (with-slots (clauses) self
    (format-char "(")
    (format-string "'Y'=(CASE")
    (dolist (clause clauses)
      (let ((when (first clause))
            (then (second clause)))
        (format-char " ")
        (if (eq when t)
            (format-string "ELSE")
            (progn
              (format-string "WHEN")
              (format-char " ")
              (format-sql-syntax-node when database)
              (format-char " ")
              (format-string "THEN")))
        (format-char " ")
        (typecase then
          (sql-literal
            (ecase (value-of then)
              ((:null nil) (format-string "'N'"))))
          (t
           (format-string "(CASE WHEN ")
           (format-sql-syntax-node then database)
           (format-string " THEN 'Y' ELSE 'N' END)")))))
    (format-char " ")
    (format-string "END)")
    (format-char ")")))

(def method format-sql-syntax-node ((x sql-drop-view) (database oracle))
  (if (ignore-missing-p x)
      (progn
        (format-string "DECLARE BEGIN EXECUTE IMMEDIATE 'DROP VIEW ")
        (format-sql-identifier (name-of x) database)
        (format-string "'; EXCEPTION WHEN OTHERS THEN NULL; END;"))
      (progn
        (format-string "DROP VIEW ")
        (format-sql-identifier (name-of x) database))))

(def method format-sql-syntax-node ((x sql-drop-table) (database oracle))
  (if (ignore-missing-p x)
      (progn
        (format-string "DECLARE BEGIN EXECUTE IMMEDIATE 'DROP TABLE ")
        (format-sql-identifier (name-of x) database)
        (format-string "'; EXCEPTION WHEN OTHERS THEN NULL; END;"))
      (progn
        (format-string "DROP TABLE ")
        (format-sql-identifier (name-of x) database))))

(def method format-sql-syntax-node ((x sql-drop-index) (database oracle))
  (if (ignore-missing-p x)
      (progn
        (format-string "DECLARE BEGIN EXECUTE IMMEDIATE 'DROP INDEX ")
        (format-sql-identifier (name-of x) database)
        (format-string "'; EXCEPTION WHEN OTHERS THEN NULL; END;"))
      (progn
        (format-string "DROP INDEX ")
        (format-sql-identifier (name-of x) database))))

(def method format-sql-syntax-node ((x sql-drop-sequence) (database oracle))
  (if (ignore-missing-p x)
      (progn
        (format-string "DECLARE BEGIN EXECUTE IMMEDIATE 'DROP SEQUENCE ")
        (format-sql-identifier (name-of x) database)
        (format-string "'; EXCEPTION WHEN OTHERS THEN NULL; END;"))
      (progn
        (format-string "DROP SEQUENCE ")
        (format-sql-identifier (name-of x) database))))

;; TODO THL class for each operator for easier dispatch?
(def method format-sql-syntax-node ((x sql-unary-operator) (database oracle))
  (with-slots (name expression) x
    (cond
      ((equal "@" name)
       (format-string "ABS(")
       (format-sql-syntax-node expression database)
       (format-string ")"))
      ((equal "|/" name)
       (format-string "SQRT(")
       (format-sql-syntax-node expression database)
       (format-string ")"))
      ((equal "NOT" name)
       (if (or (typep expression 'sql-column-alias)
               (and (typep expression 'sql-literal)
                    (typep (type-of expression) 'sql-boolean-type)))
           (progn
             (format-string "NOT('Y'=")
             (format-sql-syntax-node expression database)
             (format-string ")"))
           (call-next-method)))
      (t (call-next-method)))))

;; TODO THL class for each operator for easier dispatch?
(def method format-sql-syntax-node ((x sql-binary-operator) (database oracle))
  (with-slots (name left right) x
    (cond
      ((equal "&" name)
       (format-string "BITAND(")
       (format-sql-syntax-node left database)
       (format-string ",")
       (format-sql-syntax-node right database)
       (format-string ")"))
      ((equal "^" name)
       (format-string "(")
       (format-sql-syntax-node left database)
       (format-string "**")
       (format-sql-syntax-node right database)
       (format-string ")"))
      ((equal "=" name)
       (format-string "(")
       (let ((xblob (and (typep left 'sql-literal)
                         (typep (type-of left) 'sql-binary-large-object-type)
                         (vectorp (value-of left))
                         (value-of left)))
             (yblob (and (typep right 'sql-literal)
                         (typep (type-of right) 'sql-binary-large-object-type)
                         (vectorp (value-of right))
                         (value-of right))))
         (if (or xblob yblob)
             (flet ((blob (vector) ;; blob=
                      (loop
                         for x across vector
                         do (format *sql-stream* "~2,'0x" x))))
               (format-string "0=dbms_lob.compare(")
               (if xblob
                   (progn
                     (format-string "to_blob('")
                     (blob xblob)
                     (format-string "')"))
                   (format-sql-syntax-node left database))
               (format-string ",")
               (if yblob
                   (progn
                     (format-string "to_blob('")
                     (blob yblob)
                     (format-string "')"))
                   (format-sql-syntax-node right database))
               (format-string ")"))
             (let ((xclob (and (typep left 'sql-literal)
                               (typep (type-of left) 'sql-character-large-object-type)
                               (not (size-of (type-of left)))
                               (stringp (value-of left))
                               (value-of left)))
                   (yclob (and (typep right 'sql-literal)
                               (typep (type-of right) 'sql-character-large-object-type)
                               (not (size-of (type-of right)))
                               (stringp (value-of right))
                               (value-of right))))
               (if (or xclob yclob)
                   (progn ;; clob=
                     (format-string "0=dbms_lob.compare(")
                     (if xclob
                         (progn
                           (format-string "to_clob('")
                           (format-string xclob)
                           (format-string "')"))
                         (format-sql-syntax-node left database))
                     (format-string ",")
                     (if yclob
                         (progn
                           (format-string "to_clob('")
                           (format-string yclob)
                           (format-string "')"))
                         (format-sql-syntax-node right database))
                     (format-string ")"))
                   (progn ;; =
                     (format-sql-syntax-node left database)
                     (format-string "=")
                     (format-sql-syntax-node right database))))))
       (format-string ")"))
      (t (call-next-method)))))

(def method format-sql-syntax-node ((x sql-n-ary-operator) (database oracle))
  (with-slots (name expressions) x
    (cond
      ((member name '("AND" "OR") :test #'equal)
       (format-char "(")
       (format-separated-list expressions name database
                              (lambda (x db)
                                (if (or (typep x 'sql-column-alias)
                                        (and (typep x 'sql-literal)
                                             (typep (type-of x) 'sql-boolean-type)))
                                    (progn
                                      (format-string "('Y'=")
                                      (format-sql-syntax-node x db)
                                      (format-string ")"))
                                    (format-sql-syntax-node x db))))
       (format-char ")"))
      ((equal "||" name)
       (format-string "(")
       (format-separated-list
        expressions name database
        (lambda (x db)
          (declare (ignore db))
          (if (and (typep x 'sql-literal)
                   (typep (type-of x) 'sql-character-large-object-type)
                   (not (size-of (type-of x)))
                   (stringp (value-of x))
                   (value-of x))
              (progn
                (format-string "'")
                (format-string (value-of x))
                (format-string "'"))
              (format-sql-syntax-node x database))))
       (format-string ")"))
      (t (call-next-method)))))

(def method format-sql-syntax-node ((x sql-drop-column-action) (database oracle))
  (with-slots (name type constraints default-value cascade) x
    (format-string "DROP COLUMN ")
    (format-sql-identifier name database)
    (when cascade (format-string " CASCADE CONSTRAINTS"))))

(def method format-sql-syntax-node ((x sql-alter-column-type-action) (database oracle))
  (with-slots (name type constraints default-value) x
    (format-string "MODIFY (")
    (format-sql-identifier name database)
    (format-string " ")
    (format-sql-syntax-node type database)
    (format-string ")")))

(def method format-sql-syntax-node ((x hu.dwim.rdbms::sql-false-expression) (database oracle)) ;; TODO THL export sql-false-expression
  (format-string "1 = 2"))
