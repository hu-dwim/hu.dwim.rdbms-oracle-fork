;;; -*- mode: Lisp; Syntax: Common-Lisp; -*-
;;;
;;; Copyright (c) 2006 by the authors.
;;;
;;; See LICENCE for details.

(in-package :hu.dwim.rdbms)

(def special-variable *sql-operator-names* (list)
  "A list of symbols that name an SQL operator")

(def special-variable *sql-function-names* (list)
  "A list of symbols that name an SQL function")

;;;;;;
;;; Expressions

(def syntax-node sql-expression (sql-syntax-node)
  ())

(def syntax-node sql-query-expression (sql-expression sql-dml-statement)
  ())

;;;;;;
;;; Set operations

(defun find-column-alias (query table-name column-name)
  (find-if (lambda (column-alias)
	     (and (typep column-alias 'sql-column-alias)
		  (equal (table-of column-alias) table-name)
		  (equal (column-of column-alias) column-name)))
	   (columns-of query)))

(defun consistent-order-by (queries order-bys)
  (destructuring-bind (&optional len &rest extra)
      (remove-duplicates (mapcar #'length order-bys))
    (when (and len (plusp len) (not extra))
      ;; order-bys specified, and they have a consistent, non-zero
      ;; number of columns
      (apply #'mapcar
	     (lambda (&rest sort-specs)
	       (let* ((sort-keys (mapcar #'sort-key-of sort-specs))
		      (1st (car sort-specs))
		      (ordering (ordering-of 1st))
		      (columns (mapcar
				(lambda (q key)
				  (or (find-column-alias q (table-of key) (column-of key))
				      (error "ORDER BY does not refer to an output column: ~A" key)))
				queries
				sort-keys))
		      ;; use a name currently specified, or make a new one
		      (name (or (some #'alias-of sort-keys)
				(some #'alias-of columns)
				(gensym))))
		 (iter (for spec in sort-specs)
		       (for key in sort-keys)
		       (for col in columns)
		       (unless (eq ordering (ordering-of spec))
			 (error "inconsistent ordering in ORDER BY specs for ~A"
				key))
		       ;;
		       ;; We need (named) column aliases for sorting.
		       ;;
		       (let ((name
			      (if (typep name 'named-sql-syntax-node)
				  (name-of name)
				  name)))
			 (dolist (updateme (list key col))
			   (let* ((this-name (alias-of updateme))
				  (this-name
				   (if (typep this-name 'named-sql-syntax-node)
				       (name-of this-name)
				       this-name))) 
			     (cond
			       ((null this-name)
				(setf (alias-of updateme) name))
			       ((equal name this-name))
			       (t
				(error "inconsistent column aliases in ORDER BY specs for ~A"
				       key)))))))
		 1st))
	     order-bys))))

(def syntax-node sql-set-operation-expression (sql-query-expression)
  ((set-operation
    :type (member :union :except :intersect))
   (all
    nil
    :type boolean)
   (subqueries
    nil
    :type (list sql-query-expression))
   (order-by
    nil
    :type list)                         ; TODO: element type
   (limit
    nil
    :type (or null integer sql-literal))
   (offset
    nil
    :type (or null integer sql-literal)))
  (:format-sql-syntax-node
   ;; FIXME: little shared code between PG and ORACLE is left here.  Maybe
   ;; replace with one method in each backend directory.
   (let ((actual-subqueries (mapcar (lambda (x)
				      (etypecase x
					(sql-select x)
					(sql-subquery (query-of x))))
				    subqueries))
	 (rownump (and (or limit offset)
		      (eq (backend-type database) :oracle))))
     ;; OFFSET and LIMIT handling
     (when rownump
       (mapc #'force-aliases actual-subqueries)
       (let ((aliases (mapcar #'alias-of (columns-of (car actual-subqueries)))))
	 (format-string "SELECT ")
	 (format-comma-separated-identifiers aliases)
	 (format-string " FROM (SELECT ")
	 (format-comma-separated-identifiers aliases)
	 (format-string ", ROWNUM n FROM (")))
     (assert (notany #'limit-of actual-subqueries))
     (assert (notany #'offset-of actual-subqueries))
     ;; Kludge or Feature?
     ;;
     ;; Ordinarily, the ordering of set operations is not well-defined without
     ;; an explicit ORDER BY clauses.  We deviate from this SQL behaviour by
     ;; checking the subqueries for ORDER BY.  If the subqueries have ORDER BY
     ;; clauses that look like they make sense, we move them out to guarantee
     ;; the same ordering for the full set.
     (let* ((subquery-order-bys (mapcar #'order-by-of actual-subqueries))
	    (outer-order-by
	     (or order-by
		 (consistent-order-by actual-subqueries subquery-order-bys))))
       (ecase (backend-type database)
	 (:postgresql)
	 (:oracle
	  ;; Also, oracle doesn't support ORDER BYs in the subqueries
	  ;; (presumably because those would be pointless), so we strip
	  ;; them.
	  ;;
	  (dolist (sub actual-subqueries)
	    (setf (order-by-of sub) nil))))
       
       (format-separated-list (ecase (backend-type database)
				(:postgresql subqueries)
				(:oracle
				 ;; So ORACLE doesn't like the parens in
				 ;;   (SELECT ...) UNION (SELECT ...)
				 ;; written by SQL-SUBQUERYs.  Strip them:
				 actual-subqueries))
			      (ecase set-operation
				(:union (if all "UNION ALL" "UNION"))
				(:except (ecase (backend-type database)
					   (:postgresql "EXCEPT")
					   (:oracle "MINUS")))
				(:intersect "INTERSECT")))

       (when outer-order-by
	 (format-string " ORDER BY ")
	 (format-comma-separated-list outer-order-by)))

     (cond
       (rownump
	(mapc #'force-aliases actual-subqueries)
	(format-string ")) WHERE ")
	(when offset
	  (format-sql-syntax-node offset)
	  (format-string " < n"))
	(when limit
	  (when offset
	    (format-string " AND ")
	    (format-sql-syntax-node offset)
	    (format-string " + ")) 
	  (format-sql-syntax-node limit)
	  (format-string " >= n")))
       (t
	(when limit
	  (format-string " LIMIT ")
	  (format-sql-syntax-node limit))
	(when offset
	  (format-string " OFFSET ")
	  (format-sql-syntax-node offset)))))))

(def definer set-operation (name)
  (let ((constructor-name (sql-constructor-name name)))
    `(progn
      (pushnew ',constructor-name *sql-constructor-names*)
      (pushnew ',name *sql-operator-names*)
      (def function ,constructor-name (&rest subqueries)
        (make-instance 'sql-set-operation-expression
                       :set-operation ,(intern (symbol-name name) (find-package :keyword))
                       :subqueries subqueries)))))

(def set-operation union)

(def set-operation except)

(def set-operation intersect)

;;;;;;
;;; Operators

(def syntax-node sql-operator (sql-expression named-sql-syntax-node)
  ())

(def syntax-node sql-unary-operator (sql-operator)
  ((fix
    :prefix
    :type (member :prefix :postfix))
   (expression
    :type t #+nil sql-expression))	;also sql-identifier, number, ...
  (:format-sql-syntax-node
   (format-char "(")
   (ecase fix
     (:prefix
      (format-sql-operator-name name database)
      (format-char " ")
      (format-sql-syntax-node expression))
     (:postfix
      (format-sql-syntax-node expression)
      (format-char " ")
      (format-sql-operator-name name database)))
   (format-char ")")))

(def syntax-node sql-binary-operator (sql-operator)
  ((left
    :type t #+nil sql-expression)	;also sql-identifier, number, ...
   (right
    :type t #+nil sql-expression))
  (:format-sql-syntax-node
   (format-char "(")
   (format-sql-syntax-node left)
   (format-char " ")
   (format-sql-operator-name name database)
   (format-char " ")
   (format-sql-syntax-node right)
   (format-char ")")))

(def syntax-node sql-n-ary-operator (sql-operator)
  ((expressions
    :type list))
  (:format-sql-syntax-node
   (format-char "(")
   (format-separated-list expressions name)
   (format-char ")")))

(def definer unary-operator (name &optional (fix :prefix))
  (let ((constructor-name (sql-constructor-name name)))
    `(progn
      (pushnew ',constructor-name *sql-constructor-names*)
      (pushnew ',name *sql-operator-names*)
      (def function ,constructor-name (expression)
        (make-instance 'sql-unary-operator
                       :name ,(sql-operator-name name)
                       :fix ,fix
                       :expression expression)))))

(def definer binary-operator (name)
  (let ((constructor-name (sql-constructor-name name)))
    `(progn
      (pushnew ',constructor-name *sql-constructor-names*)
      (pushnew ',name *sql-operator-names*)
      (def function ,constructor-name (left right)
        (make-instance 'sql-binary-operator
                       :name ,(string-upcase name)
                       :left left
                       :right right)))))

(def definer n-ary-operator (name)
  (let ((constructor-name (sql-constructor-name name)))
    `(progn
      (pushnew ',constructor-name *sql-constructor-names*)
      (pushnew ',name *sql-operator-names*)
      (def function ,constructor-name (&rest expressions)
        (make-instance 'sql-n-ary-operator
                       :name ,(string-upcase name)
                       :expressions expressions)))))

(def definer varary-operator (name)
  (let ((constructor-name (sql-constructor-name name)))
    `(progn
      (pushnew ',constructor-name *sql-constructor-names*)
      (pushnew ',name *sql-operator-names*)
      (def function ,constructor-name (&rest expressions)
        (if (length= 1 expressions)
            (make-instance 'sql-unary-operator
                           :name ,(string-upcase name)
                           :expression (first expressions))
            (make-instance 'sql-n-ary-operator
                           :name ,(string-upcase name)
                           :expressions expressions))))))

;;;;;;
;;; Logical operators

(def unary-operator not)

(def n-ary-operator and)

(def n-ary-operator or)

;;;;;;
;;; Set operators

(def binary-operator in)

;;;;;;
;;; Comparison operators

(def binary-operator =)

(def binary-operator <)

(def binary-operator >)

(def binary-operator <=)

(def binary-operator >=)

(def binary-operator <>)

(def unary-operator is-null :postfix)

(def unary-operator is-not-null :postfix)

;;;;;;
;;; Arithmetic operators

(def varary-operator +)

(def varary-operator -)

(def n-ary-operator *)

(def binary-operator /)

(def binary-operator %)

(def binary-operator ^)

(def n-ary-operator \|\|)

(def unary-operator \|/)

(def unary-operator @)

;;;;;;
;;; Bitwise operators

(def binary-operator &)

(def binary-operator \|)

(def binary-operator \#)

(def unary-operator ~)

(def binary-operator |<<|)

(def binary-operator |>>|)

;;;;;;
;;; Pattern matching

(def syntax-node sql-like (sql-expression)
  ((string :type (or sql-expression sql-column-alias) :accessor string-of)
   (pattern :type (or sql-literal sql-expression))
   (case-sensitive-p t :type boolean))
  (:format-sql-syntax-node
   (if case-sensitive-p
       (progn
         (format-char "(")
         (format-sql-syntax-node string)
         (format-string " LIKE ")
         (format-sql-syntax-node pattern)
         (format-char ")"))
       (progn
         (format-string "(UPPER(")
         (format-sql-syntax-node string)
         (format-string ") LIKE UPPER(")
         (format-sql-syntax-node pattern)
         (format-string "))")))))

(def syntax-node sql-regexp-like (sql-expression)
  ((string :type sql-expression :accessor string-of)
   (pattern :type sql-expression)
   (case-sensitive-p #t :type boolean)))

;;;;;;
;;; Case expressions

(def syntax-node sql-case (sql-expression)
  ((clauses :type list))
  (:format-sql-syntax-node
   (format-char "(")
   (format-string "CASE")
   (dolist (clause clauses)
     (let ((when (first clause))
           (then (second clause)))
       (format-char " ")
       (if (eq when t)
           (format-string "ELSE")
           (progn
             (format-string "WHEN")
             (format-char " ")
             (format-sql-syntax-node when)
             (format-char " ")
             (format-string "THEN")))
       (format-char " ")
       (format-sql-syntax-node then)))
   (format-char " ")
   (format-string "END")
   (format-char ")")))

(def function sql-cond (clauses)
  (sql-case :clauses clauses))

(def function sql-if (cond then else)
  (sql-case :clauses `((,cond ,then) (t ,else))))

;;;;;;
;;; Subquery expressions

(def syntax-node sql-subquery (sql-query-expression)
  ((query
    ;; TODO: extract query-expression from the ddl statement
    :type (or ;; DFL maybe this should be SQL-QUERY-EXPRESSION?
	      sql-select sql-set-operation-expression)))
  (:format-sql-syntax-node
   (format-char "(")
   (format-sql-syntax-node query)
   (format-char ")")))

(defgeneric first-columns-of (sql-qe))

(defmethod first-columns-of ((qe sql-set-operation-expression))
  (iter (for sub-qe in (subqueries-of qe))
	(appending (first-columns-of sub-qe))))

(defmethod first-columns-of ((qe sql-subquery))
  (first-columns-of (query-of qe)))

(def syntax-node sql-project-to (sql-query-expression)
  ((query :type sql-subquery)
   (column))
  (:format-sql-syntax-node
   (let ((result-set (gensym))
	 (col-alias (gensym)))

     (dolist (col-obj (first-columns-of (query-of (the sql-subquery query))))
       (setf (alias-of col-obj) col-alias))

     (format-string "(SELECT ")
     (format-sql-identifier (make-instance 'sql-column-alias
					   :table result-set
					   :column col-alias))
     (format-string " FROM ")
     (format-sql-syntax-node query)
     (format-sql-identifier result-set)
     (format-string ")"))))

(def unary-operator exists)

;;;;;;
;;; Functions

(def syntax-node sql-function-call (sql-expression)
  ((name
    :type (or string symbol))
   (arguments
    nil))
  (:format-sql-syntax-node
   (format-sql-function-name name database)
   (format-char "(")
   (format-comma-separated-list arguments)
   (format-char ")")))

(def definer aggregate-function (name)
  (let ((constructor-name (sql-constructor-name name)))
    `(progn
      (pushnew ',constructor-name *sql-constructor-names*)
      (pushnew ',name *sql-function-names*)
      (def function ,constructor-name (&rest arguments)
        (make-instance 'sql-function-call
                       :name ,(string-upcase name)
                       :arguments arguments)))))

;;;;;;
;;; Aggregate functions

(def aggregate-function count)

(def aggregate-function distinct)

(def aggregate-function min)

(def aggregate-function max)

(def aggregate-function avg)

(def aggregate-function sum)

;;;;;;
;;; Count(*)

(def function sql-count-* ()
  ;; TODO the sql-all-columns ctor macro is not yet defined here (select.lisp)
  (sql-count (make-instance 'sql-all-columns)))

;;;;;;
;;; false expression

(def syntax-node sql-false-expression (sql-query-expression)
  ()
  (:format-sql-syntax-node
   (format-sql-literal
    (sql-literal :value #f :type (make-instance 'sql-boolean-type)))))

;;; full text search queries
;;;
;;; http://www.stanford.edu/dept/itss/docs/oracle/10g/text.101/b10730/cqoper.htm
;;; http://www.postgresql.org/docs/8.4/static/textsearch.html
;;;
;;; SQL-FULL-TEXT-SEARCH-QUERY is a wrapper representing a "datatype"
;;; for full text queries.  It provides a sexp-based syntax for those
;;; queries but a string can be used directly too if portability is
;;; not required.  The query is represented as a string in the
;;; resulting sql code.

(def syntax-node sql-full-text-search-query (sql-syntax-node)
  ((query))
  (:format-sql-syntax-node
   (format-sql-syntax-node
    (full-text-search-query-to-sql query (backend-type database)))))

(def method format-sql-literal ((q sql-full-text-search-query) db)
  (format-sql-syntax-node q db))

(defun normalize-full-text-search-query (q)
  (labels ((rec (x)
             (if (atom x)
                 (when x
                   (if (stringp x)
                       (unless (equal "" x)
                         x)
                       (ecase x
                         ((:one :any) x))))
                 (destructuring-bind (head &rest tail) x
                   (ecase head
                     (:within
                      (let ((exp (rec (car tail)))
                            (domains (cdr tail)))
                        (cond
                          ((and exp domains) `(,head ,exp ,@domains))
                          (exp exp))))
                     ((:and :or)
                      (let (z)
                        (dolist (y tail)
                          (let ((yy (rec y)))
                            (when yy
                              (pushnew yy z :test #'equal))))
                        (when z
                          (if (not (cdr z))
                              (car z)
                              `(,head ,@(nreverse z))))))
                     (:not
                      (let ((y (rec (car tail))))
                        (when y
                          `(,head ,y))))
                     (:wild
                      (let (z pre)
                        (dolist (y tail)
                          (let ((yy (rec y)))
                            (when yy
                              (unless (and (eq :any yy) (eq :any pre))
                                (setq pre yy)
                                (push yy z)))))
                        (when z
                          (unless (equal '(:any) z)
                            `(,head ,@(nreverse z))))))
                     (:seq x))))))
    (rec q)))

;; TODO THL shouldn't this be part of sexp2sql and rdbms dependent on it?
(defun full-text-search-query-to-sql (q backend)
  (with-output-to-string (s)
    (labels ((p (args &optional lpar sep rpar)
               (when lpar
                 (princ lpar s))
               (loop
                  for arg in args
                  for n from 0
                  do (progn
                       (when (and sep (plusp n))
                         (princ sep s))
                       (rec arg)))
               (when rpar
                 (princ rpar s)))
             (rec (q &optional top)
               (etypecase q
                 (keyword
                  ;; wildcards don't seem to work on postgresql
                  (assert (not (eq :postgresql backend)))
                  (princ (ecase q
                           (:one "_")
                           (:any "%"))
                         s))
                 (string
                  (assert (not (find #\space q :test #'char=)))
                  ;; TODO THL wildcard as non-wildcard escaping on oracle
                  (princ q s))
                 (cons (cond
                         ((eq :wild (car q))
                          (p (cdr q)))
                         ((eq :within (car q)) ;; http://download.oracle.com/docs/cd/B19306_01/text.102/b14218/cqoper.htm#i998525
                          (let ((x (cadr q))
                                (domains (cddr q)))
                            (if (cdr domains)
                                (rec (cons :or
                                           (loop
                                              for d in domains
                                              collect `(:within ,x ,d))))
                                (p (list x) nil nil (format nil " within ~a" (car domains)))))) ;; TODO THL escape section name e.g. _ see doc?
                         ((cddr q)
                          (p (cdr q)
                             (if (and (eq :postgresql backend)
                                      (eq :seq (car q)))
                                 ;; "''" is something else then it
                                 ;; seems to be: This is primarily
                                 ;; useful when the configuration
                                 ;; includes a thesaurus dictionary
                                 ;; that may trigger on such phrases.
                                 (error ":SEQ not supported on PostgreSQL")
                                 (unless top "("))
                             (ecase backend
                               (:postgresql
                                (ecase (car q)
                                  (:and "&")
                                  (:or "|")
                                  #+nil (:seq " ")))
                               (:oracle
                                (ecase (car q)
                                  (:and " and ")
                                  (:or "|")
                                  (:seq " "))))
                             (if (and (eq :postgresql backend)
                                      (eq :seq (car q)))
                                 ;; "''"
                                 (error ":SEQ not supported on PostgreSQL")
                                 (unless top ")"))))
                         ((eq :not (car q))
                          (ecase backend
                            (:postgresql (p (cadr q) "!"))
                            (:oracle (p (cadr q) "~"))))
                         (t (rec (cadr q) top)))))))
      (rec q t))))

;;; SQL-FULL-TEXT-SEARCH-QUERY-OUTER-FUNCTION and
;;; SQL-FULL-TEXT-SEARCH-QUERY-INNER-FUNCTION
;;;
;;; Because full-text search on postgresql doesn't support phrase and
;;; wildcard searching, these two functions extend the basic search by
;;; building more complicated sql clauses depending on the query.
;;;
;;; To build portable full text search queries with "full"
;;; functionality (term, phrase and wildcard search), use these
;;; SQL-SYNTAX-NODEs only with backends that require query
;;; manipulation like in the case of postgresql (e.g. use backend-case
;;; macro in hu.dwim.perec at higher level in the perec query).

(def syntax-node sql-full-text-search-query-outer-function (sql-syntax-node)
  ((exp)
   (what)
   (query))
  (:format-sql-syntax-node
   (error
    "SQL-FULL-TEXT-SEARCH-QUERY-OUTER-FUNCTION ~s ~s ~s not meant to be used with ~s backend"
    exp what query (backend-type *database*))))

(def syntax-node sql-full-text-search-query-inner-function (sql-syntax-node)
  ((exp))
  (:format-sql-syntax-node
   (error
    "SQL-FULL-TEXT-SEARCH-QUERY-INNER-FUNCTION ~s not meant to be used with ~s backend"
    exp (backend-type *database*))))
