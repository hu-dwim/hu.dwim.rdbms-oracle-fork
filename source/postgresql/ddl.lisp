;;; -*- mode: Lisp; Syntax: Common-Lisp; -*-
;;;
;;; Copyright (c) 2006 by the authors.
;;;
;;; See LICENCE for details.

(in-package :hu.dwim.rdbms.postgresql)

(def function list-objects (type)
  (map 'list [elt !1 0] (execute (format nil "SELECT relname FROM pg_class WHERE relkind = '~A'" type))))

(def method database-list-sequences ((database postgresql))
  (list-objects "S"))

(def method database-list-tables ((database postgresql))
  (list-objects "r"))

(def method database-list-views ((database postgresql))
  (list-objects "v"))

(def method database-list-table-columns (name (database postgresql))
  (map 'list
       (lambda (column)
         (make-instance 'sql-column
                        :name (first-elt column)
                        :type (sql-type-for-internal-type (subseq column 1))))
       (execute
        (format nil "SELECT pg_attribute.attname, pg_type.typname, pg_attribute.attlen,
                                   pg_attribute.atttypmod, pg_attribute.attnotnull
                            FROM pg_type, pg_class, pg_attribute
                            WHERE pg_class.oid = pg_attribute.attrelid AND
                                  pg_class.relname = '~A' AND
                                  pg_attribute.attname NOT IN ('cmin', 'cmax', 'xmax', 'xmin', 'oid', 'ctid', 'tableoid') AND
                                  pg_attribute.attisdropped = FALSE AND
                                  pg_attribute.atttypid = pg_type.oid"
                (string-downcase name)))))

(def method database-list-table-indices (name (database postgresql))
  (map 'list
       (lambda (column)
         (make-instance 'sql-index
                        :name (first-elt column)
                        :table-name name))
       (execute
        (format nil "select indexname from pg_indexes where tablename = '~A'"
                (string-downcase name)))))

(def method database-list-dependent-views (table column (database postgresql))
  (execute
   (format nil "select distinct c3.relname
                  from pg_attribute a
                       JOIN pg_class c ON (a.attrelid=c.oid)
                       JOIN pg_depend d ON (d.refobjid=c.oid AND d.refobjsubid=a.attnum)
                       JOIN pg_class c2 ON (d.classid=c2.oid AND c2.relname='pg_rewrite')
                       JOIN pg_rewrite r ON (d.objid=r.oid)
                       JOIN pg_class c3 ON (r.ev_class=c3.oid)
                  where c.relname='~A' and a.attname='~A' AND c3.relkind='v'"
           (string-downcase table) (string-downcase column))))

(def method database-list-table-primary-constraints (name (database postgresql))
  (remove-if-not (lambda (x) (search "pkey" x))
                 (list-table-indices name)
                 :key #'name-of))

(defun sql-rule-name-to-lisp (str)
  (let ((sym (find-symbol (string-upcase str) :keyword)))
    (case sym
      (:no\ action :defer-restrict)
      (:set\ null :set-null)
      (:set\ default :set-default)
      ((:cascade :restrict) sym)
      (t (error "invalid action: ~A" str)))))

(def method database-list-table-foreign-keys (table-name (database postgresql))
  (map 'list
       (lambda (row)
	 (make-instance 'foreign-key-descriptor
			:name (elt row 0)
			:source-table (elt row 1)
			:source-column (elt row 2)
			:target-table (elt row 3)
			:target-column (elt row 4)
			:delete-rule (sql-rule-name-to-lisp (elt row 5))
			:update-rule (sql-rule-name-to-lisp (elt row 6))))
       (execute
	(format nil "SELECT DISTINCT
                  tc.constraint_name,
                  tc.table_name,
                  kcu.column_name, 
                  ccu.table_name AS foreign_table_name,
                  ccu.column_name AS foreign_column_name,
                  rc.delete_rule,
                  rc.update_rule
                FROM
                  information_schema.table_constraints AS tc 
                  JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.constraint_catalog = kcu.constraint_catalog
                  JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                    AND ccu.constraint_catalog = tc.constraint_catalog
                  JOIN information_schema.referential_constraints AS rc
                    ON rc.constraint_name = tc.constraint_name
                    AND rc.constraint_catalog = tc.constraint_catalog
                WHERE constraint_type = 'FOREIGN KEY'
                  AND tc.constraint_catalog='~A'
                  AND tc.table_name='~A';"
		(getf (connection-specification-of *database*)
		      :database)
		(string-downcase table-name)))))
