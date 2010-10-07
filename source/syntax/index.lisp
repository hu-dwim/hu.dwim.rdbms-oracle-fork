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
    :type list))
  (:documentation "An SQL index specification."))

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
   (format-string " (")
   (format-comma-separated-list
    columns
    (lambda (node db)
      (typecase node
	(sql-column
	  (funcall 'format-sql-identifier node db))
	(t
	 ;; Oracle doesn't permit table_name.column_name in
	 ;; index expressions, and the table_name is redundant
	 ;; anyway, so let's strip it unconditionally:
	 (labels ((shorten-columns (node)
		    (etypecase node
		      (sql-function-call
			(setf (arguments-of node)
			      (mapcar #'shorten-columns
				      (arguments-of node))))
		      (sql-column-alias
			(setf (table-of node) nil)))
		    node))
	   (funcall 'format-sql-syntax-node (shorten-columns node) db))))))
   (format-char ")")))

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
