;;; -*- mode: Lisp; Syntax: Common-Lisp; -*-
;;;
;;; Copyright (c) 2006 by the authors.
;;;
;;; See LICENCE for details.

(in-package :hu.dwim.rdbms)

(def syntax-node sql-index (named-sql-syntax-node)
  ((unique
    #f
    :type boolean)
   (table-name
    :type sql-identifier*)
   (columns
    ;; allows columns or full expressions (if the database supports that)
    nil
    :type list)
   (where
    ;; e.g. http://www.postgresql.org/docs/8.4/static/indexes-partial.html
    nil
    :type list)
   (using
    ;; e.g. [USING method] for postgresql
    ;; http://www.postgresql.org/docs/8.2/static/sql-createindex.html
    nil
    :type string)
   (properties
    ;; index properties for oracle
    ;; http://download.oracle.com/docs/cd/B13789_01/server.101/b10759/statements_5010.htm#i2138869
    nil
    :type string)
   (triggers nil :type list)) ;; list of strings
  (:documentation "An SQL index specification."))

;; Oracle doesn't permit table_name.column_name in index expressions,
;; and the table_name is redundant anyway, so let's strip it
;; unconditionally:
(defun %shorten-columns (node)
  (etypecase node
    (sql-literal)
    (sql-fragment) ;; allow sexp2sql
    (sql-unary-operator
      (setf (expression-of node)
            (%shorten-columns (expression-of node))))
    (sql-function-call
      (setf (arguments-of node)
            (mapcar #'%shorten-columns (arguments-of node))))
    (sql-index-operation
      (setf (value-of node) (%shorten-columns (value-of node))))
    (sql-column-alias
      (setf (table-of node) nil)))
  node)

(def syntax-node sql-create-index (sql-ddl-statement sql-index)
  ()
  (:documentation "An SQL CREATE INDEX statement.")
  (:format-sql-syntax-node
   (format-string "CREATE ")
   (when unique
     (format-string "UNIQUE "))
   (format-string "INDEX ")
   (format-sql-identifier name)
   (format-string " ON ")
   (format-sql-identifier table-name)
   (when using
     (format-string " USING ")
     (format-string using))
   (format-string " (")
   (format-comma-separated-list
    columns
    (lambda (node db)
      (typecase node
	(sql-column (funcall 'format-sql-identifier node db))
	(t (funcall 'format-sql-syntax-node (%shorten-columns node) db)))))
   (format-char ")")
   (when where
     (format-string " WHERE ")
     (funcall 'format-sql-syntax-node (%shorten-columns where) *database*))
   (when properties
     (format-string " ")
     (format-string properties))))

(def syntax-node sql-index-operation (sql-syntax-node)
  ((value
    :type sql-syntax-node)
   (operation
    ;; http://developer.postgresql.org/pgdocs/postgres/indexes-opclass.html
    :type sql-identifier*))
  (:documentation "An expression for CREATE INDEX annotated with an index operation.")
  (:format-sql-syntax-node
   (format-sql-syntax-node value)
   (format-string " ")
   (format-sql-identifier operation)))

(def syntax-node sql-drop-index (sql-ddl-statement)
  ((name
    :type string)
   (ignore-missing
    #f
    :type boolean))
  (:documentation "An SQL DROP INDEX statement.")
  (:format-sql-syntax-node
    (format-string "DROP INDEX ")
    (when ignore-missing
      (format-string "IF EXISTS "))
    (format-sql-identifier name)))
