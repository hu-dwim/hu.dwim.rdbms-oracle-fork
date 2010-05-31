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

(def method database-list-views ((database oracle))
  (mapcar #'first (execute "select view_name from user_views" :result-type 'list)))

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
    (format nil "select column_name, data_type, char_length, data_precision, data_scale from user_tab_columns where table_name = '~A'"
            name)
    :result-type 'vector)))

(def method database-list-table-indices (name (database oracle))
  (mapcar
   (lambda (column)
     (make-instance 'sql-index
                    :name (first column)
                    :table-name name
                    :unique (equal "UNIQUE" (second column))))
   (execute
    (format nil "select index_name, uniqueness from user_indexes where table_name = '~A'"
            name)
    :result-type 'list)))

(def method database-list-table-primary-constraints (name (database oracle))
  (mapcar
   (lambda (column) (make-instance 'sql-constraint :name (first column)))
   (execute
    (format nil "select constraint_name from user_constraints where constraint_type='P' and table_name='~A'"
            name)
    :result-type 'list)))

(defun sql-rule-name-to-lisp (str)
  (let ((sym (find-symbol (string-upcase str) :keyword)))
    ;; Oracle doesn't have RESTRICT, only NO ACTION, so for compatibility
    ;; with the PostgreSQL backend, let's pretend that NO ACTION is
    ;; the same as RESTRICT rather than  :DEFER-RESTRICT.
    (case sym
      (:cascade :cascade)
      (:set\ null :set-null)
      (:no\ action :restrict)
      (t (error "invalid action: ~A" str)))))

(def method database-list-table-foreign-keys (table-name (database oracle))
  (map 'list
       (lambda (row)
	 (make-instance 'foreign-key-descriptor
			:name (elt row 0)
			:source-table (elt row 1)
			:source-column (elt row 2)
			:target-table (elt row 3)
			:target-column (elt row 4)
			:delete-rule (sql-rule-name-to-lisp (elt row 5))
			;; pretend that update and delete are the same:
			:update-rule (sql-rule-name-to-lisp (elt row 5))))
       (execute
	(format nil "SELECT c3.constraint_name,
                            c1.table_name, c1.column_name,
                            c2.table_name, c2.column_name,
                            c4.delete_rule
                       FROM user_tab_columns c1
                         JOIN user_cons_columns c3
                           ON c1.table_name = c3.table_name
                              AND c1.column_name = c3.column_name
                         JOIN user_constraints c4
                           ON c3.constraint_name = c4.constraint_name
                         JOIN user_cons_columns c2
                           ON c4.r_constraint_name = c2.constraint_name
                              AND c3.position = c2.position
                     WHERE c1.table_name='~A'
                           AND c4.constraint_type = 'R'"
		(string-downcase table-name)))))
