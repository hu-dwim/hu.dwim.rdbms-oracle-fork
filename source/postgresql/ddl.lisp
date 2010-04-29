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
