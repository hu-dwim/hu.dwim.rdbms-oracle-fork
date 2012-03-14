;;; -*- mode: Lisp; Syntax: Common-Lisp; -*-
;;;
;;; Copyright (c) 2006 by the authors.
;;;
;;; See LICENCE for details.

(in-package :hu.dwim.rdbms)

(def syntax-node sql-create-table (sql-ddl-statement)
  ((name
    :type sql-identifier*)
   (temporary
    nil
    :type (or boolean (member (:drop :preserve-rows :delete-rows))))
   (columns
    nil
    :type list)
   (as
    nil
    :type (or null sql-select sql-set-operation-expression)))
  (:documentation "An SQL CREATE TABLE statement.")
  (:format-sql-syntax-node
   (format-string "CREATE")
   (when temporary
     ;; TODO global always?
     (format-string " GLOBAL TEMPORARY"))
   (format-string " TABLE ")
   (format-sql-identifier name)
   (format-string " (")
   (format-comma-separated-list columns)
   (format-char ")")
   (when (and temporary (not (eq temporary #t)) (not as))
     (format-string " ON COMMIT ")
     (format-string (ecase temporary
                      (:drop "DROP")
                      (:preserve-rows "PRESERVE ROWS")
                      (:delete-rows "DELETE ROWS"))))
   (when as
     (format-string " AS ")
     (format-sql-syntax-node as))))

(def syntax-node sql-create-view (sql-create-table)
  ((replace
    #f
    :type boolean
    :accessor replace-p))
  (:documentation "An SQL CREATE TABLE statement.")
  (:format-sql-syntax-node
   (format-string "CREATE")
   (when replace
     (format-string " OR REPLACE"))
   (when temporary
     (format-string " TEMPORARY"))
   (format-string " VIEW ")
   (format-sql-identifier name)
   (awhen columns
     (format-string " (")
     (format-comma-separated-identifiers columns)
     (format-char ")"))
   (format-string " AS ")
   (format-sql-syntax-node as)))

;;; We need to distinguish between constraints which can be set up
;;; during CREATE TABLE and those which require a second ALTER TABLE
;;; step:
;;;
;;; While foreign key constraints are nominally part of the column
;;; definition, it is not possible to define both together in the case
;;; where the target table has not been defined yet, meaning that
;;; ordering matterns and circular references need to be dealt with.
;;;
;;; An similar issue would arise for table contents: We need to be able
;;; to separate out table and constraint definition for data import files,
;;; where the order must be:
;;;  1. create table
;;;  2. insert into / copy
;;;  3. alter table add foreign key
;;;
(defun delay-constraint-until-alter-table-p (constraint)
  (check-type constraint sql-constraint)
  (typep constraint 'sql-foreign-key-constraint))

(def syntax-node sql-column (named-sql-syntax-node)
  ((type
    :type sql-type)
   (constraints
    nil
    :type list)
   (default-value
    :type t)
   (oid-default-statement		;always delayed as alter table
    nil
    :type (or null sql-add-oid-column-default)
    :accessor oid-default-statement-of))
  (:documentation "An SQL column specification.")
  (:format-sql-syntax-node
   (format-sql-identifier name)
   (when type
     (format-char " ")
     (format-sql-syntax-node type))
   (when (slot-boundp -self- 'default-value)
     (format-string " DEFAULT ")
     (format-sql-literal default-value))
   (mapc (lambda (constraint)
	   (unless (delay-constraint-until-alter-table-p constraint)
	     (format-sql-syntax-node constraint)))
	 constraints))
  (:format-sql-identifier
   (format-sql-identifier name)))

(def syntax-node sql-add-oid-column-default (sql-ddl-statement)
  ((table-name
    :type sql-identifier*)
   (column-name
    :type sql-identifier*)
   (class-id
    :type integer))
  (:documentation "An SQL ALTER TABLE statement to add a sequence-provided default value to the oid column."))
