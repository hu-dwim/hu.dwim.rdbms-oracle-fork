;;; -*- mode: Lisp; Syntax: Common-Lisp; -*-
;;;
;;; Copyright (c) 2006 by the authors.
;;;
;;; See LICENCE for details.

(in-package :hu.dwim.rdbms.oracle)

(def method database-list-sequences ((database oracle))
  (mapcar #'first (execute "select sequence_name from user_sequences" :result-type 'list)))

(def method database-list-tables ((database oracle))
  (mapcar #'first (execute "select table_name from user_tables" :result-type 'list)))

(def method database-list-table-columns (name (database oracle))
  (map 'list
   (lambda (column)
     (make-instance 'sql-column
                    :name (aref column 0)
                    :type (sql-type-for-internal-type
                           (aref column 1)
                           (aref column 2)
                           (aref column 3)
                           (aref column 4))))
   (execute
    (format nil "select column_name, data_type, char_length, data_precision, data_scale from user_tab_columns where lower(table_name) = '~A'"
            (string-downcase name)) ;; FIXME should be case sensitive, but perec
                                    ;; does not use case consistently
    :result-type 'vector)))

(def method database-list-table-indices (name (database oracle))
  (mapcar
   (lambda (column)
     (make-instance 'sql-index
                    :name (first column)
                    :table-name name))
   (execute
    (format nil "select index_name from user_indexes where lower(table_name) = '~A'"
            (string-downcase name)) ;; FIXME see prev
    :result-type 'list)))
