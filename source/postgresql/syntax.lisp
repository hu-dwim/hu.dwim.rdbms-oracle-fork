;;; -*- mode: Lisp; Syntax: Common-Lisp; -*-
;;;
;;; Copyright (c) 2006 by the authors.
;;;
;;; See LICENCE for details.

(in-package :hu.dwim.rdbms.postgresql)

(def method format-sql-syntax-node ((action sql-add-column-action) (database postgresql))
  (format-string "ADD ")
  (format-sql-identifier (name-of action) database)
  (format-char " ")
  (format-sql-syntax-node (type-of action) database))

(def method format-sql-syntax-node ((node sql-add-oid-column-default)
				    (database postgresql))
  (with-slots (class-id table-name column-name) node
      (format *sql-stream*
	      "ALTER TABLE ~A ALTER COLUMN ~A SET DEFAULT ((NEXTVAL('_instance_id') << ~D) | ~D)"
	      table-name
	      column-name
	      hu.dwim.perec::+oid-class-id-bit-size+
	      class-id)))

(def method format-sql-literal ((literal vector) (database postgresql))
  (format-string "E'")
  (loop for el across literal
        do (if (or (<= 0 el 31)
                   (<= 127 el 255)
                   (= el #.(char-code #\'))
                   (= el #.(char-code #\\)))
               (format *sql-stream* "\\\\~3,'0o" el)
               (format-char (code-char el))))
  (format-string "'::bytea"))

(def method format-sql-literal ((literal sql-literal) (database postgresql))
  (if (unquote-aware-format-sql-literal literal)
      (progn
        (format-string "$")
        (format-string (princ-to-string (length *binding-types*)))
        (format-string "::")
        (format-sql-syntax-node (type-of literal) database))
      (call-next-method)))

(def method format-sql-syntax-node ((variable sql-binding-variable) (database postgresql))
  (unquote-aware-format-sql-binding-variable variable)
  (format-string "$")
  (format-string (princ-to-string (length *binding-types*)))
  (awhen (type-of variable)
    (format-string "::")
    (format-sql-syntax-node (type-of variable) database)))

(def method format-sql-syntax-node ((like sql-like) (database postgresql))
  (with-slots (string pattern case-sensitive-p) like
    (format-char "(")
    (format-sql-syntax-node string database)
    (if case-sensitive-p
        (format-string " LIKE ")
        (format-string " ILIKE "))
    (format-sql-syntax-node pattern database)
    (format-char ")")))

(def method format-sql-syntax-node ((regexp-like sql-regexp-like) (database postgresql))
  (format-char "(")
  (format-sql-syntax-node (string-of regexp-like) database)
  (format-char " ")
  (format-string (if (case-sensitive-p regexp-like) "~" "~*"))
  (format-char " ")
  (format-sql-syntax-node (pattern-of regexp-like) database)
  (format-char ")"))

(def method equal-type-p ((type-1 sql-binary-large-object-type) (type-2 sql-binary-large-object-type) (database postgresql))
  ;; don't compare size, because postgresql has no fixed size binary, so it can't be extracted from the schema
  (eq (class-of type-1) (class-of type-2)))

(def method equal-type-p ((type-1 sql-integer-type) (type-2 sql-numeric-type) (database postgresql))
  (not (bit-size-of type-1)))

(def method equal-type-p ((type-1 sql-numeric-type) (type-2 sql-integer-type) (database postgresql))
  (not (bit-size-of type-2)))

(def method format-sql-syntax-node
    ((node sql-character-varying-type/without-size-if-possible)
     (database postgresql))
  (format-string "CHARACTER VARYING")
  ;; no SIZE needed in PostgreSQL
  )

(defmethod equal-type-p
    ((type-1 sql-character-varying-type/without-size-if-possible)
     (type-2 sql-character-varying-type)
     (database postgresql))
   t)

;;; SQL-FULL-TEXT-SEARCH-QUERY-OUTER-FUNCTION and
;;; SQL-FULL-TEXT-SEARCH-QUERY-inner-FUNCTION play some tricks
;;; together.  The purpose of the outer function is to postpone sql
;;; query construction until runtime because only then I know what the
;;; sql query should look like depending on the value (full text
;;; search query) of the query lexical variable.  Anything under the
;;; outer function cannot have SQL-UNQUOTEs except the inner function,
;;; which then "replaces" the SQL-UNQUOTE with the actual runtime
;;; value.

(defparameter *inner-function-replacement* nil)

(defun the-unquoted-lexical-variable (x) ;; TODO THL the perec:: symbols here?!
  (assert (typep x 'sql-unquote))
  (let ((form (form-of x)))
    (unless (and (listp form)
		 (eq (car form) 'hu.dwim.perec::value->sql-literal))
      (error "sql-test does not support the argument ~A" form))
    (destructuring-bind (var type args)
	(cdr form)
      (declare (ignore type args))
      (unless (typep var 'hu.dwim.perec::lexical-variable)
	(error "sql-test does not support the variable ~A" var))
      var)))

(def method format-sql-syntax-node
  ((x sql-full-text-search-query-outer-function) (database postgresql))
  (let ((value-form (let ((q (query-of x)))
                      (etypecase q
                        (hu.dwim.rdbms::sql-unquote
                          (the-unquoted-lexical-variable q))
                        (hu.dwim.rdbms::sql-literal
                          (value-of q))))))
    (push-form-into-command-elements
     `(let ((*inner-function-replacement* ,value-form))
        (format-sql-syntax-node
         (rewrite-full-text-search-query-outer-function
          ,(exp-of x) ,(what-of x) ,value-form)
         *database*)))))

(def method format-sql-syntax-node
  ((x sql-full-text-search-query-inner-function) (database postgresql))
  ;; Here I ignore the SQL-UNQUOTE because it's to late for it to be
  ;; useful.  I use the unquoted value passed in from the outer
  ;; function instead.
  (format-sql-syntax-node *inner-function-replacement* database))

(defun rewrite-full-text-search-query-outer-function (exp what query)
  ;; Similar to full-text-search-query-to-sql but this time creating
  ;; syntax-nodes and fixing the actual query.
  ;;
  ;; IMPORTANT: For the pattern search to work, the data must be a
  ;; list of words all on one line separated by single space.  The
  ;; first word must be preceded by a space and the last word must be
  ;; followed by a space.  It is the responsibility of the application
  ;; to provide data in this format.
  (labels ((sql-pattern (x)
             (sql-binary-operator :name "~"
                                  :left what
                                  :right (format nil "[ ]~a[ ]" x)))
           (update (words patterns)
             (setf (query-of query) `(:and ,@words))
             (apply 'sql-and
                    (append (when words (list exp))
                            (mapcar #'sql-pattern patterns))))
           (ugly (form)
             (multiple-value-bind (words patterns)
                 (words-and-patterns-of-full-text-query form) ;; this fn is ugly
               (update words patterns)))
           (rec (q)
             (if (atom q)
                 (ugly `(:and ,q))
                 (ecase (car q)
                   (:and (ugly q))
                   #+nil(:or (ugly `(:and ,q))) ;; TODO THL the rest of the cases
                   (:not (ugly `(:and ,q)))
                   (:wild (ugly `(:and ,q)))
                   (:seq (ugly `(:and ,q)))))))
    (let ((q (query-of query)))
      (if q
          (rec q)
          (sql-literal :value t :type (sql-boolean-type) :suppress-unquoting t)))))

(defun words-and-patterns-of-full-text-query (q)
  (let ((words nil)
        (patterns nil))
    (assert (eq :and (car q)))
    (dolist (x (cdr q))
      (etypecase x
        (string (push x words)) ;; pg doesn't interpret wildcards here
        (cons (labels ((escape-regexp (x s)
                         (loop
                            for c across x
                            do (princ (case c
                                        (#\. "[.]")
                                        (#\* "[*]")
                                        (#\^ "[^]")
                                        (#\$ "[$]")
                                        (#\( "[(]")
                                        (#\) "[)]")
                                        (#\| "[|]")
                                        (#\\ "[\\]")
                                        (#\[ "\\[")
                                        (#\] "\\]")
                                        (t c))
                                      s)))
                       (wild (x)
                         (with-output-to-string (s)
                           (dolist (y (cdr x))
                             (case y
                               (:one (princ "[^ ]" s))
                               (:any (princ "[^ ]*" s))
                               (t (escape-regexp y s)))))))
                (ecase (car x)
                  (:seq (dolist (y (cdr x))
                          (when (atom y)
                            (push y words)))
                        (push (with-output-to-string (s)
                                (loop
                                   for y in (cdr x)
                                   for n from 0
                                   do (progn
                                        (when (plusp n)
                                          (princ "[ ]" s))
                                        (etypecase y
                                          (string (escape-regexp y s))
                                          (cons
                                           (ecase (car y)
                                             (:wild (princ (wild y) s))))))))
                              patterns))
                  (:wild (push (wild x) patterns)))))))
    (values (nreverse words)
            (nreverse patterns))))
