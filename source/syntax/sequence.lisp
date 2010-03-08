;;; -*- mode: Lisp; Syntax: Common-Lisp; -*-
;;;
;;; Copyright (c) 2006 by the authors.
;;;
;;; See LICENCE for details.

(in-package :hu.dwim.rdbms)

(def syntax-node sql-create-sequence (sql-ddl-statement)
  ((name
    :type string)
   (temporary
    #f
    :type boolean))
  (:documentation "An SQL CREATE SEQUENCE statement.")
  (:format-sql-syntax-node
   (format-string "CREATE")
   (when temporary
     (format-string " TEMPORARY"))
   (format-string " SEQUENCE ")
   (format-sql-identifier name)))

(def syntax-node sql-drop-sequence (sql-ddl-statement)
  ((name
    :type string))
  (:documentation "An SQL DROP SEQUENCE statement.")
  (:format-sql-syntax-node
   (format-string "DROP")
   (format-string " SEQUENCE ")
   (format-sql-identifier name)))

(def syntax-node sql-sequence-nextval-column (sql-syntax-node)
  ((name
    :type string))
  (:documentation "An SQL SEQUENCE next value column.")
  (:format-sql-syntax-node
   (format-string "NEXTVAL('")
   (format-sql-identifier name)
   (format-string "')")))
