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

;;; David's divert-command-elements code (Finding the unquote's
;;; binding variable index)

(defun divert-command-elements (node)
  (let ((*command-elements* (make-array 1 :fill-pointer 1 :adjustable t)))
    (vector-push-extend (with-output-to-string (*sql-stream*)
			  (format-sql-syntax-node node *database*))
			*command-elements*)
    (map 'list
	 (lambda (x) (if (stringp x) `(write-string ,x *sql-stream*) x))
	 *command-elements*)))

(defun invoke-diverting-command-elements (fun arg-node)
  (let ((v (divert-command-elements arg-node)))
    (push-form-into-command-elements
     `(let* ((my)
             (idx (length *binding-types*))
	     (diverted-sql
	      (with-output-to-string (*sql-stream*)
		(let ((*binding-variables* (make-array 16 :adjustable #t :fill-pointer 0))
                      (*binding-types* (make-array 16 :adjustable #t :fill-pointer 0))
                      (*binding-values* (make-array 16 :adjustable #t :fill-pointer 0)))
                  (describe (second (second (third ',v))))
                  (print (list '========== ,(second (second (third v)))))
                  ,@v
                  (assert (eql (length *binding-types*) 1))
                  (setf my (vector-pop *binding-values*))))))
	(assert (eql (length *binding-types*) idx #+nil(1+ idx)))
	,(funcall fun 'my #+nil`(elt *binding-values* idx))))))

(defmacro diverting-command-elements ((value-getter arg-node) &body body)
  `(invoke-diverting-command-elements (lambda (,value-getter) ,@body)
				      ,arg-node))

(defmacro write-diverted-sql ()
  `(write-string diverted-sql *sql-stream*))

(defun check-unquoted-lexical-variable (syntax-node)
  (assert (typep syntax-node 'sql-unquote))
  (let ((form (form-of syntax-node)))
    (unless (and (listp form)
		 (eq (car form) 'hu.dwim.perec::value->sql-literal))
      (error "sql-test does not support the argument ~A" form))
    (destructuring-bind (var type args)
	(cdr form)
      (declare (ignore type args))
      (unless (typep var 'hu.dwim.perec::lexical-variable)
	(error "sql-test does not support the variable ~A" var)))))

(defun the-unquoted-lexical-variable (x)
  (check-unquoted-lexical-variable x)
  x)

;;; SQL-FULL-TEXT-SEARCH-QUERY-OUTER-FUNCTION

(def method format-sql-syntax-node
  ((x sql-full-text-search-query-outer-function) (database postgresql))
  (diverting-command-elements
      (value-form (the-unquoted-lexical-variable (hu.dwim.rdbms::query-of x)))
    `(if t #+nil(search "test" ,value-form)
         (progn
           (print (list '@@@@@@@@@-1 ,value-form (baumdb-impl::one ,value-form)))
           ;;(setf (baumdb-impl::one ,value-form) "YES")
           (setf ,value-form "TRUE")
           (print (list '@@@@@@@@@-2 ,value-form))
           ;;(print (list '@@@@@@@@@-2 ,value-form (baumdb-impl::one ,value-form)))
           ;; (print (list '@@@@@@@@@-1 ,value-form))
           ;; (print (list '@@@@@@@@@-1 (query-of ,value-form)))
           ;; ;;(print (list '@@@@@@@@@-2 (setf (query-of ,value-form) "YES")))
           ;; (print (list '@@@@@@@@@-3 (setf ,value-form "YES")))
           ;;(format-string "'<<<' || '")
           ;;(write-diverted-sql)
           (format-string ,value-form)
           ;;(format-string "YES")
           ;;(format-sql-syntax-node (baumdb-impl::one ,value-form) *database*)
           ;;(format-sql-syntax-node ,value-form *database*)
           ;;(format-string "' || '>>>'")
           #+nil
           (setf ,value-form (concatenate 'string
                                          "["
                                          ,value-form
                                          "|"
                                          ,value-form
                                          "]")))
         (write-diverted-sql)))
  #+nil
  (let* ((var (hu.dwim.rdbms::var-of x))
         (val (cdar *full-text-search-query-bindings*))) ;; TODO THL find the right value based on var
    (format-sql-syntax-node
     (rewrite-full-text-search-query-outer-function (exp-of x)
                                                    (hu.dwim.rdbms::what-of x)
                                                    var
                                                    val
                                                    (query-of val))
     database)))

;; (def method format-sql-syntax-node
;;   ((x sql-full-text-search-query-inner-function) (database postgresql))
;;   (let ((exp (exp-of x)))
;;     ;;(error "INNER ~s" *full-text-search-query-bindings*)
;;     (if *%finding-inner-lvar*
;;         exp
;;         (format-sql-syntax-node exp database))))

(defun rewrite-full-text-search-query-outer-function (exp what var val query)
  ;; similar to full-text-search-query-to-sql but this time creating
  ;; syntax-nodes and fixing the actual query
  (labels ((rec (q)
             (etypecase q
               ;; TODO THL the string case
               ;; (string
               ;;  (assert (not (find #\space q :test #'char=)))
               ;;  ;; TODO THL wildcard as non-wildcard escaping on oracle
               ;;  (princ q s))
               (cons (ecase (car q)
                       (:and
                        (multiple-value-bind (words patterns)
                            (words-and-patterns-of-full-text-query q)
                          (setf (query-of val) `(:and ,@ words))
                          (apply 'sql-and
                                 (append (when words
                                           (list exp))
                                         (loop
                                            for pattern in patterns
                                            collect (sql-like
                                                      :string what
                                                      :pattern pattern
                                                      :case-sensitive-p nil))))))
                       ;; TODO THL the rest of the cases
                       #+nil(:or)
                       #+nil(:not)
                       #+nil(:seq)
                       #+nil(:wild))))))
    (rec query)))

(defun words-and-patterns-of-full-text-query (q)
  ;; Q has a restricted form (for now): one which allows term, exact
  ;; phrase (without wildcards) and single word wildcard search.  For
  ;; example: (:AND "hello" "hi" (:WILD "wild1" :ANY) (:WILD :ANY
  ;; "wild2") (:SEQ "einmal" "vor") (:SEQ "zweimal%" "nach")).  The
  ;; trick here is that we can split the query into two parts: one
  ;; full text search on words and the other one a LIKE query on the
  ;; (possibly array) of patterns.  Anything more complicated would
  ;; most likely mean building and/or/not logic expressions on top of
  ;; @@ and LIKE expressions.
  (let ((words nil)
        (patterns nil))
    (assert (eq :and (car q)))
    (dolist (x (cdr q))
      (etypecase x
        (string (push x words)) ;; pg doesn't interpret wildcards here
        (cons (flet ((escape (x s)
                       (loop
                          for c across x
                          do (princ (case c
                                      (#\_ "\\_")
                                      (#\% "\\%")
                                      (t c))
                                    s))))
                (ecase (car x)
                  (:seq (dolist (y (cdr x))
                          (push y words))
                        (push (with-output-to-string (s)
                                (princ "%" s)
                                (loop
                                   for y in (cdr x)
                                   for n from 0
                                   do (progn
                                        (when (plusp n)
                                          (princ " " s))
                                        (etypecase y
                                          (string (escape y s)))))
                                (princ "%" s))
                              patterns))
                  (:wild (push (with-output-to-string (s)
                                 (dolist (y (cdr x))
                                   (case y
                                     (:one (princ "_" s))
                                     (:any (princ "%" s))
                                     (t (escape y s)))))
                               patterns)))))))
    (values (nreverse words) (nreverse patterns))))
