;;; -*- mode: Lisp; Syntax: Common-Lisp; -*-
;;;
;;; Copyright (c) 2006 by the authors.
;;;
;;; See LICENCE for details.

(in-package :hu.dwim.rdbms.oracle)

(cffi:define-foreign-library oracle-oci
  (:windows (:or "ocixe.dll" "oci.dll"))
  (t (:default #-x86-64 "libocixe" #+x86-64 "libclntsh"))) ;; TODO THL dont use libocixe?

(def special-variable *oracle-oci-foreign-library* nil)

(def function ensure-oracle-oci-is-loaded ()
  (unless *oracle-oci-foreign-library*
    ;; TODO let the user control version, path and stuff (through slots on *database*? if so then *oracle-oci-foreign-library* must be a slot there, too)
    (setf *oracle-oci-foreign-library*
          (cffi:load-foreign-library
           'oracle-oci
           :search-path (list
                         #-x86-64
                         "/usr/lib/oracle/xe/app/oracle/product/10.2.0/client/lib/"
                         #+x86-64
                         "/u01/app/oracle/product/11.2.0/xe/lib/")))))

;;;;;;
;;; Backend API

(def method begin-transaction ((database oracle) (transaction oracle-transaction))
  ;; nop, because oracle implicitly has transactions
  )

(def method commit-transaction ((database oracle) (transaction oracle-transaction))
  (oci-call (oci:trans-commit (service-context-handle-of transaction)
                              (error-handle-of transaction)
                              *default-oci-flags*)))

(def method rollback-transaction ((database oracle) (transaction oracle-transaction))
  (oci-call (oci:trans-rollback (service-context-handle-of transaction)
                                (error-handle-of transaction)
                                *default-oci-flags*)))

(def method prepare-command ((database oracle)
                            (transaction oracle-transaction)
                            (command string)
                            &key &allow-other-keys)
  (ensure-connected transaction)
  (rdbms.debug "Preparing command: ~S" command)
  (make-prepared-statement command))

(def method execute-command ((database oracle)
                            (transaction oracle-transaction)
                            (command string)
                            &key visitor binding-types binding-values result-type
                            &allow-other-keys)
  (rdbms.debug "Executing ~S" command)
  (let ((statement (prepare-command database transaction command)))
    (unwind-protect
         (execute-prepared-statement transaction statement binding-types binding-values visitor result-type)
      (free-prepared-statement statement))))

(def method execute-command ((database oracle)
                            (transaction oracle-transaction)
                            (prepared-statement prepared-statement)
                            &key visitor binding-types binding-values result-type
                            &allow-other-keys)
  (execute-prepared-statement transaction prepared-statement binding-types binding-values visitor result-type))

(def method cleanup-transaction :after ((transaction oracle-transaction))
  (when (environment-handle-pointer transaction)
    (rdbms.debug "Cleaning up Oracle transaction ~A to database ~A" transaction (database-of transaction))
    (disconnect transaction)))

;;;;;;
;;; Connection

(def function ensure-connected (transaction)
  (when (cl:null (environment-handle-pointer transaction))
    (connect transaction)))

(def class* oci-environment ()
  (env-handle
   (pools :initform (make-hash-table :test 'equal))))

(def class* oci-pool ()
  (pool-handle
   pool-name
   pool-name-len))

(defvar *environments* (make-hash-table))

(defun flush-environments (&key do-not-free)
  (iter (for (key env) in-hashtable *environments*)
	(unless do-not-free
	  (oci-call (oci:handle-free (env-handle-of env) oci:+htype-env+)))
	(remhash key *environments*)))

(defun create-oci-environment (desired-encoding)
  (rdbms.debug "Setting up environment for ~A" desired-encoding)
  (cffi:with-foreign-object (&env :pointer)
    (oci-call (oci:env-create &env
			      (logior (ecase desired-encoding
					(:ascii 0)
					(:utf-16 oci:+utf-16+))
				      *default-oci-flags*)
			      null null null null 0 null))
    (make-instance 'oci-environment
		   :env-handle (cffi:mem-ref &env :pointer))))

(defun ensure-oci-environment (desired-encoding)
  (or (gethash desired-encoding *environments*)
      (setf (gethash desired-encoding *environments*)
	    (create-oci-environment desired-encoding))))

(defun create-connection-pool (environment dblink user password)
  (rdbms.debug "Setting up pool for ~A" environment)
  (cffi:with-foreign-objects ((&error-handle :pointer)
			      (&pool :pointer)
			      (&pool-name :pointer)
			      (&pool-name-len 'oci:sb-4))
    (handle-alloc &error-handle oci:+htype-error+)
    (handle-alloc &pool oci:+htype-cpool+)
    (let ((error-handle (cffi:mem-ref &error-handle :pointer))
	  (pool (cffi:mem-ref &pool :pointer)))
      (with-foreign-oci-string (dblink c-dblink l-dblink)
	(with-foreign-oci-string (user c-user l-user)
	  (with-foreign-oci-string (password c-password l-password)
	    (oci::connection-pool-create (env-handle-of environment)
					 error-handle
					 pool
					 &pool-name &pool-name-len
					 c-dblink l-dblink
					 0 100 1
					 c-user l-user
					 c-password l-password
					 oci:+default+))))
      (make-instance 'oci-pool
		     :pool-handle pool
		     :pool-name (oci-string-to-lisp
				 (cffi:mem-ref &pool-name :pointer)
				 (cffi:mem-ref &pool-name-len 'oci:sb-4))))))

(defun ensure-connection-pool (environment dblink user password)
  (let ((table (pools-of environment))
	(key (list dblink user password)))
    (or (gethash key table)
	(setf (gethash key table)
	      (create-connection-pool environment dblink user password)))))

(defvar *use-connection-pool* t)

(def function connect (transaction)
  (assert (cl:null (environment-handle-pointer transaction)))
  (ensure-oracle-oci-is-loaded)
  (bind ((environment (ensure-oci-environment
		       (connection-encoding-of (database-of *transaction*))))
	 ((&key datasource user-name (password "") schema) (connection-specification-of (database-of transaction))))
    (macrolet ((alloc (&rest whats)
                 `(progn
                   ,@(loop for what :in whats
                           for accessor = (format-symbol (find-package :hu.dwim.rdbms.oracle) "~A-POINTER" what)
                           collect `(setf (,accessor transaction)
                                     (make-void-pointer))))))
      (alloc
       environment-handle
       error-handle
       server-handle
       service-context-handle
       session-handle))

    (setf (cffi:mem-ref (environment-handle-pointer transaction) :pointer)
	  (env-handle-of environment))

    (rdbms.debug "Connecting in transaction ~A" transaction)
    (handle-alloc (error-handle-pointer transaction) oci:+htype-error+)
    (handle-alloc (server-handle-pointer transaction) oci:+htype-server+)
    (handle-alloc (service-context-handle-pointer transaction) oci:+htype-svcctx+)

    (iter connecting
          (with-simple-restart (retry "Retry connecting to Oracle")
            (rdbms.debug "Logging on in transaction ~A" transaction)
            (if *use-connection-pool*
		(server-attach-using-pool (ensure-connection-pool
					   environment
					   datasource
					   user-name
					   password))
		(server-attach datasource))

            (oci-call (oci:attr-set (service-context-handle-of transaction)
                                    oci:+htype-svcctx+
                                    (server-handle-of transaction)
                                    0
                                    oci:+attr-server+
                                    (error-handle-of transaction)))

            (handle-alloc (session-handle-pointer transaction) oci:+htype-session+)

            (set-session-string-attribute oci:+attr-username+ user-name)
            (set-session-string-attribute oci:+attr-password+ password)

            (oci-call (oci:session-begin (service-context-handle-of transaction)
                                         (error-handle-of transaction)
                                         (session-handle-of transaction)
                                         oci:+cred-rdbms+
                                         oci:+default+))

            (oci-call (oci:attr-set (service-context-handle-of transaction)
                                    oci:+htype-svcctx+
                                    (session-handle-of transaction)
                                    0
                                    oci:+attr-session+
                                    (error-handle-of transaction)))
            (return-from connecting))
          (unless (cffi:null-pointer-p (session-handle-of transaction))
            (oci-call (oci:handle-free (session-handle-of transaction) oci:+htype-session+))
            (setf (session-handle-of transaction) null)))
    (when schema
      (setf (session-schema transaction) schema)
      (execute-command (database-of transaction)
		       transaction
		       (format nil "ALTER SESSION SET CURRENT_SCHEMA=~A"
			       schema)))))

(def macro ignore-errors* (&body body)
  `(block nil
    (handler-bind ((serious-condition
                    (lambda (error)
                      (rdbms.warn "Ignoring error: ~A" error)
                      (return))))
      ,@body)))

(def function disconnect (transaction)
  (assert (environment-handle-pointer transaction))

  (ignore-errors*
    (rdbms.debug "Calling logoff in transaction ~A" transaction)
    (oci-call (oci:logoff (service-context-handle-of transaction)
                          (error-handle-of transaction))))

  #+nil					;now global
  (ignore-errors*
    (rdbms.debug "Freeing environment handle of transaction ~A" transaction)
    (oci-call (oci:handle-free (environment-handle-of transaction) oci:+htype-env+)))

  (macrolet ((dealloc (&rest whats)
               `(progn
                 ,@(loop for what in whats
                         for accessor = (format-symbol (find-package :hu.dwim.rdbms.oracle) "~A-POINTER" what)
                         collect `(awhen (,accessor transaction)
                                   (cffi:foreign-free it)
                                   (setf (,accessor transaction) nil))))))
    (dealloc
     #+nil environment-handle		;now global
     error-handle
     server-handle
     service-context-handle
     session-handle)))

;;;;;;
;;; Prepared statement

(def function make-prepared-statement (command &optional (name ""))
  (let ((statement (make-instance 'oracle-prepared-statement
                                  :name name
                                  :statement-handle-pointer (make-void-pointer)
                                  :query command)))
    (handle-alloc (statement-handle-pointer statement) oci:+htype-stmt+)
    (stmt-prepare statement command)
    (rdbms.dribble "Statement is allocated")
    statement))

(def function free-prepared-statement (statement)
  (oci-call (oci:handle-free (statement-handle-of statement) oci:+htype-stmt+))
  (free-bindings (bindings-of statement))
  (cffi:foreign-free (statement-handle-pointer statement)))

(defun decode-row (column-descriptors result-type)
  (ecase result-type
    (list
     (loop
        for d across column-descriptors
        collect (fetch-column-value d 0)))
    (vector
     (loop
        with vals = (make-array (length column-descriptors))
        for d across column-descriptors
        for i from 0
        do (setf (aref vals i) (fetch-column-value d 0))
        finally (return vals)))))

(defun call-with-column-descriptors (statement transaction fn)
  (let ((d (make-column-descriptors statement transaction)))
    (unwind-protect (funcall fn d)
      (loop
         for x across d
         do (free-column-descriptor transaction x)))))

(defmacro with-column-descriptors ((var statement transaction) &body body)
  `(call-with-column-descriptors ,statement ,transaction (lambda (,var) ,@body)))

(defun execute-prepared-statement (transaction statement binding-types binding-values visitor result-type)
  (progn ;; TODO THL remove progn, kept only to preserve indentation
    ;; make bindings
    (setf (bindings-of statement) (make-bindings statement transaction binding-types binding-values))

    ;; TODO THL configurable prefetching limits?
    (set-statement-attribute statement oci:+attr-prefetch-rows+ 1000000)
    (set-statement-attribute statement oci:+attr-prefetch-memory+ #. (* 10 (expt 2 20)))
    
    ;; execute
    (stmt-execute statement *default-oci-flags*)

    ;; fetch
    (cond
      ((select-p statement)
       (with-column-descriptors (d statement transaction)
         (flet ((fetch ()
                  (when (stmt-fetch-2 statement 1 oci:+fetch-next+ 0)
                    (assert (eql 1 (get-statement-attribute
                                    statement
                                    oci:+attr-rows-fetched+
                                    'oci:ub-4)))
                    t))
                (row ()
                  (decode-row d result-type)))
           (if visitor
               (loop
                  while (fetch)
                  do (funcall visitor (row)))
               (ecase result-type
                 (list
                  (loop
                     while (fetch)
                     collect (row)))
                 (vector
                  (loop
                     with v = (make-array 8 :adjustable t :fill-pointer 0)
                     while (fetch)
                     do (vector-push-extend (row) v)
                     finally (return v))))))))
      (t
       (when (and (or (insert-p statement)
                      (update-p statement))
                  binding-types
                  binding-values)
         (loop
            for type across binding-types
            for value across binding-values
            for binding in (bindings-of statement)
            when (and (lob-type-p type)
                      (not (member value '(:null nil #()) :test #'equalp)))
            do (upload-lob (cffi:mem-aref (data-pointer-of binding) :pointer) value)))
       (values nil (get-row-count-attribute statement)))))) ;; TODO THL what should the first value be?

;;;;;;
;;; Binding

(def class* oracle-binding ()
  ((bind-handle-pointer)
   (sql-type)
   (typemap)
   (data-pointer)
   (data-size)
   (indicator)))

(def function make-bindings (statement transaction binding-types binding-values)
  (iter (for type :in-vector binding-types)
        (for value :in-vector binding-values)
        (for position :from 1)
        (collect (make-binding statement transaction position type value))))

(def function make-binding (statement transaction position sql-type value)
  (let* ((statement-handle (statement-handle-of statement))
         (error-handle (error-handle-of transaction))
         (typemap (typemap-for-sql-type sql-type))
         (oci-type-code (typemap-external-type typemap))
         (converter (typemap-lisp-to-oci typemap))
         (bind-handle-pointer (foreign-alloc-with-initial-element :pointer :initial-element null))
         (is-null (or (eql value :null)
                      (and (cl:null value) (not (typep sql-type 'sql-boolean-type)))))
         (indicator (foreign-alloc-with-initial-element 'oci:sb-2 :initial-element (if is-null -1 0))))
    (multiple-value-bind (data-pointer data-size)
        (if is-null
            (if (or (typep sql-type 'sql-character-large-object-type)
                    (typep sql-type 'sql-binary-large-object-type))
                (make-lob-locator t) ;; TODO THL why needed when indicator is -1?
                (values null 0))
            (funcall converter value))

      (rdbms.dribble "Value ~S converted to ~A" value (dump-c-byte-array data-pointer data-size))

      ;; TODO THL why **locator and not *locator? stmt-execute crashes:-{
      (when (lob-type-p sql-type)
        (setq data-pointer (foreign-alloc-with-initial-element :pointer :initial-element data-pointer)))
      
      (oci-call (oci:bind-by-pos statement-handle
                                 bind-handle-pointer
                                 error-handle
                                 position
                                 data-pointer
                                 data-size
                                 oci-type-code
                                 indicator
                                 null               ; alenp
                                 null               ; rcodep
                                 0                  ; maxarr_len
                                 null               ; curelep
                                 *default-oci-flags*))
      (make-instance 'oracle-binding
                     :bind-handle-pointer bind-handle-pointer
                     :sql-type sql-type
                     :typemap typemap
                     :data-pointer data-pointer
                     :data-size data-size
                     :indicator indicator))))

(def function free-bindings (bindings)
  (mapc 'free-binding bindings))

(def function free-binding (binding)
  (cffi:foreign-free (bind-handle-pointer-of binding))
  (cffi:foreign-free (indicator-of binding))
  (let ((data-pointer (data-pointer-of binding)))
    (unless (cffi:null-pointer-p data-pointer)
      (cffi:foreign-free data-pointer))))

;;;;;;
;;; Cursor

(def constant +number-of-buffered-rows+ 1) ; TODO prefetching rows probably superfluous, because OCI does that

(def class* column-descriptor ()
  ((define-handle-pointer)
   (name)
   (size)
   (buffer)
   (typemap)
   (indicators)
   (return-codes)
   (paraminfo)))

(defun ensure-allocator (cache count)
  (if (< count (length cache))
      (or (elt cache count)
	  (setf (elt cache count)
		(compile
		 nil
		 `(lambda ()
		    (declare (optimize speed))
		    (let ((ptr (cffi:foreign-alloc :uint8 :count ,count)))
		      (dotimes (i ,count)
			(setf (cffi:mem-aref ptr :uint8 i) 0))
		      ptr)))))
      (progn
	(lambda ()
	  (warn "out of line call to foreign-alloc for count ~D" count)
	  (cffi:foreign-alloc :uint8
			      :count count
			      :initial-element 0)))))

(defvar *paraminfo-lock* (bt:make-recursive-lock))

(defun clear-paraminfo-cache ()
  (clrhash (paraminfos-of *database*)))

(defun acquire-paraminfo
    (column-type column-size precision scale database)
  (let ((key (list column-type column-size precision scale))
	(table (paraminfos-of database)))
    (bt:with-recursive-lock-held (*paraminfo-lock*)
      (let ((values (gethash key table)))
	(cond
	  (values
	   (setf (gethash key table) (cdr values))
	   (car values))
	  (t
	   nil))))))

(defun return-paraminfo (paraminfo database)
  (setf (acquire-paraminfo (paraminfo-column-type paraminfo)
			   (paraminfo-column-size paraminfo)
			   (paraminfo-precision paraminfo)
			   (paraminfo-scale paraminfo)
			   database)
	paraminfo))

(defun (setf acquire-paraminfo)
    (newval column-type column-size precision scale database)
  (let ((key (list column-type column-size precision scale))
	(table (paraminfos-of database)))
    (bt:with-recursive-lock-held (*paraminfo-lock*)
      (push newval (gethash key table))))
  newval)

(def function initialize-buffer-for-column (constructor buffer size number-of-rows)
  (when constructor
    (loop for i from 0 below number-of-rows
       do (funcall constructor (cffi:inc-pointer buffer (* i size))))))

(def function allocate-buffer-for-column (typemap column-size number-of-rows)
  "Returns buffer, buffer-size, constructor"
  (let* ((external-type (typemap-external-type typemap))
	 (size (data-size-for external-type column-size))
	 (funcache (load-time-value (make-array 10000 :initial-element nil)))
	 (ptr (funcall (ensure-allocator funcache (* size number-of-rows))))
	 (constructor (typemap-allocate-instance typemap)))

    (initialize-buffer-for-column constructor
				  ptr
				  size
				  number-of-rows)

    (values
     ptr
     size
     constructor)))

(def function free-column-descriptor (transaction descriptor)
  (with-slots (size typemap buffer paraminfo) descriptor
    (let ((destructor (typemap-free-instance typemap)))
      (when destructor
        (loop for i from 0 below +number-of-buffered-rows+
              do (funcall destructor (cffi:mem-ref buffer :pointer (* i size)))))
      (return-paraminfo paraminfo (database-of transaction)))))

(def class* oracle-cursor (cursor)
  ((statement)
   (column-descriptors)
   (current-position 0)
   (buffer-start-position 0)
   (buffer-end-position 0)))

(def generic column-descriptor-of (cursor index)
  (:method ((cursor oracle-cursor) index)
           (svref (column-descriptors-of cursor) index)))

(def class* oracle-sequential-access-cursor (oracle-cursor sequential-access-cursor)
  ((end-seen #f :type boolean)))

(def class* oracle-random-access-cursor (oracle-cursor random-access-cursor)
  ((row-count nil)))

;;;;;;
;;; Cursor API

(def method make-cursor ((transaction oracle-transaction)
                        &key statement random-access-p &allow-other-keys)
  (if random-access-p
      (aprog1 (make-instance 'oracle-random-access-cursor
                             :statement statement
                             :column-descriptors (make-column-descriptors statement transaction))
        (row-count it)  ; TODO This positions to the last row so the response
                            ;      time can be high. Try to delay this computation
        (rdbms.dribble "Count of rows: ~D" (row-count-of it)))
      (make-instance 'oracle-sequential-access-cursor
                     :statement statement
                     :column-descriptors (make-column-descriptors statement transaction))))

(def method close-cursor ((cursor oracle-cursor))
  (loop for descriptor across (column-descriptors-of cursor)
        do (free-column-descriptor (transaction-of cursor) descriptor)))

(def method cursor-position ((cursor oracle-sequential-access-cursor))
  (unless (end-seen-p cursor)
    (current-position-of cursor)))

(def method cursor-position ((cursor oracle-random-access-cursor))
  (with-slots (current-position row-count) cursor
    (when (and (<= 0 current-position) (< current-position row-count))
      current-position)))

(def method (setf cursor-position) (where (cursor oracle-sequential-access-cursor))
  (ecase where
    (:first (setf (current-position-of cursor) 0))
    (:next (incf (current-position-of cursor))))
  (ensure-current-position-is-buffered cursor)) ; TODO delay this call

(def method (setf cursor-position) (where (cursor oracle-random-access-cursor))
  (if (integerp where)
      (incf (current-position-of cursor) where)
      (ecase where
        (:first (setf (current-position-of cursor) 0))
        (:last (setf (current-position-of cursor) (1- (row-count cursor))))
        (:previous (decf (current-position-of cursor)))
        (:next (incf (current-position-of cursor))))))

(def method absolute-cursor-position ((cursor oracle-cursor))
  (current-position-of cursor)) ; FIXME always the same as (cursor-position cursor) ?

(def method (setf absolute-cursor-position) (where (cursor oracle-random-access-cursor))
  (setf (current-position-of cursor) where))

(def method row-count ((cursor oracle-sequential-access-cursor))
  (error "Row count not supported by ~S" cursor))

(def method row-count ((cursor oracle-random-access-cursor))
  (with-slots (row-count statement buffer-start-position buffer-end-position) cursor
    (unless row-count
      (if (stmt-fetch-last statement)
          (setf row-count (get-row-count-attribute statement)
                buffer-start-position (1- row-count)
                buffer-end-position row-count)
          (setf row-count 0)))
    row-count))

(def method column-count ((cursor oracle-cursor))
  (length (column-descriptors-of cursor)))

(def method column-name ((cursor oracle-cursor) index)
  (name-of (column-descriptor-of cursor index)))

(def method column-type ((cursor oracle-cursor) index)
  nil) ; TODO

(def method column-value ((cursor oracle-cursor) index)
  (when (ensure-current-position-is-buffered cursor)
    (fetch-column-value (column-descriptor-of cursor index)
                        (- (current-position-of cursor)
                           (buffer-start-position-of cursor)))))

#|
(def function current-row (cursor &key result-type)
  (when (ensure-current-position-is-buffered cursor)
    (with-slots (column-descriptors current-position buffer-start-position default-result-type) cursor
      (let ((row-index (- current-position buffer-start-position)))
        (ecase (or result-type default-result-type)
          (vector (aprog1 (make-array (length column-descriptors))
                    (loop for descriptor across column-descriptors
                          for column-index from 0
                          do (setf (svref it column-index)
                                   (fetch-column-value descriptor row-index)))))
          (list (loop for descriptor across column-descriptors
                      collect (fetch-column-value descriptor row-index))))))))
|#

;;;;;;
;;; Cursor helpers

(def function make-column-descriptors (statement transaction)
  (coerce (cffi:with-foreign-objects ((param-descriptor-pointer :pointer))
            (loop for column-index from 1  ; OCI 1-based indexing
                  while (eql (oci:param-get (statement-handle-of statement)
                                            oci:+htype-stmt+
                                            (error-handle-of transaction)
                                            param-descriptor-pointer
                                            column-index)
                             oci:+success+)
                  collect (make-column-descriptor statement
                                                  transaction
                                                  column-index
                                                  (cffi:mem-ref param-descriptor-pointer :pointer))))
          'simple-vector))

(defstruct (paraminfo (:constructor %make-paraminfo))
  typemap
  define-handle-pointer
  return-codes
  indicators
  buffer
  constructor
  size
  ;; hash key:
  column-type
  column-size
  precision
  scale)

(defun %parse-paraminfo (param-descriptor)
  (cffi:with-foreign-objects ((attribute-value :uint8 8) ; 8 byte buffer for attribute values
                              (attribute-value-length 'oci:ub-4))
    (macrolet
	;; use a macro so that the compiler macro on mem-ref can see the
	;; cffi-type!  essential for speed.
	((%oci-attr-get (attribute-id cffi-type)
	   `(progn
	      (oci-attr-get param-descriptor ,attribute-id attribute-value attribute-value-length)
	      (cffi:mem-ref attribute-value ,cffi-type)))
	 (oci-string-attr-get (attribute-id)
	   `(progn
	      (oci-attr-get param-descriptor ,attribute-id attribute-value attribute-value-length)
	      (oci-string-to-lisp
	       (cffi:mem-ref attribute-value :pointer) ; OraText*
	       (cffi:mem-ref attribute-value-length 'oci:ub-4)))))
      (let ((column-name (oci-string-attr-get oci:+attr-name+))
	    (column-type (%oci-attr-get oci:+attr-data-type+ 'oci:ub-2))
	    (column-size)
	    (precision)
	    (scale))
	(declare (fixnum column-type))
	(progn
	  ;; KLUDGE oci:+attr-data-size+ returned as ub-2, despite it is documented as ub-4
	  (setf (cffi:mem-ref attribute-value :unsigned-short) 0)
	  (setf column-size (%oci-attr-get oci:+attr-data-size+ 'oci:ub-2)))

	(rdbms.dribble "Retrieving column: name=~W, type=~D, size=~D"
		       column-name column-type column-size)

	(when (= column-type oci:+sqlt-num+)
	  ;; the type of the precision attribute is 'oci:sb-2, because we
	  ;; use an implicit describe here (would be sb-1 for explicit describe)
	  (setf precision (%oci-attr-get oci:+attr-precision+ 'oci:sb-2)
		scale (%oci-attr-get oci:+attr-scale+ 'oci:sb-1)))
	(values column-name column-type column-size precision scale)))))

(defun make-paraminfo (column-type column-size precision scale)
  (let ((typemap (typemap-for-internal-type column-type column-size :precision precision :scale scale)))
    (multiple-value-bind (buffer size constructor)
	(allocate-buffer-for-column
	 typemap column-size +number-of-buffered-rows+)
      (%make-paraminfo
       :typemap typemap
       :define-handle-pointer (foreign-alloc-with-initial-element :pointer :initial-element null)
       :return-codes (cffi:foreign-alloc :unsigned-short :count +number-of-buffered-rows+)
       :indicators (cffi:foreign-alloc :short :count +number-of-buffered-rows+)
       :buffer buffer
       :constructor constructor
       :size size
       :column-type column-type
       :column-size column-size
       :precision precision
       :scale scale))))

(defun parse-paraminfo (param-descriptor database)
  (multiple-value-bind (column-name column-type column-size precision scale)
      (%parse-paraminfo param-descriptor)
    (values column-name
	    (let ((x (acquire-paraminfo
		      column-type column-size precision scale database)))
	      (cond
		(x
		 (initialize-buffer-for-column
		  (paraminfo-constructor x)
		  (paraminfo-buffer x)
		  (paraminfo-size x)
		  +number-of-buffered-rows+)
		 x)
		(t
		 (make-paraminfo column-type column-size precision scale)))))))

(def function make-column-descriptor (statement transaction position param-descriptor)
  (multiple-value-bind (column-name paraminfo)
      (parse-paraminfo param-descriptor
		       (database-of transaction))
    (let ((typemap (paraminfo-typemap paraminfo))
	  (define-handle-pointer (paraminfo-define-handle-pointer paraminfo))
	  (return-codes (paraminfo-return-codes paraminfo))
	  (indicators (paraminfo-indicators paraminfo))
	  (buffer (paraminfo-buffer paraminfo))
	  (size (paraminfo-size paraminfo)))
      (oci:define-by-pos
	  (statement-handle-of statement)
	  define-handle-pointer
	(error-handle-of transaction)
	position
	buffer
	size
	(typemap-external-type typemap)
	indicators
	null
	return-codes
	*default-oci-flags*)
      (make-instance 'column-descriptor
		     :define-handle-pointer define-handle-pointer
		     :name column-name
		     :size size
		     :buffer buffer
		     :typemap typemap
		     :return-codes return-codes
		     :indicators indicators
		     :paraminfo paraminfo))))

(def generic ensure-current-position-is-buffered (cursor)
  (:method ((cursor oracle-sequential-access-cursor))
           (with-slots (statement current-position
                                  buffer-start-position buffer-end-position end-seen) cursor
             (cond
               ((< current-position buffer-start-position)
                (if (zerop current-position)
                    nil                 ; TODO reexecute statement
                    (error "Trying to go backwards with a sequential cursor: ~S" cursor)))
               (t
                (loop until (or (> buffer-end-position current-position)
                                end-seen)
                  do (if (stmt-fetch-next statement +number-of-buffered-rows+)
                         (setf buffer-start-position buffer-end-position
                               buffer-end-position (get-row-count-attribute statement))
                         (setf end-seen #t))
                  finally (return-from ensure-current-position-is-buffered (not end-seen)))))))

  (:method ((cursor oracle-random-access-cursor))
           (with-slots (statement current-position buffer-start-position buffer-end-position
                                  row-count) cursor
             (cond
               ((or (< current-position 0)
                    (>= current-position row-count))
                #f)
               ((>= current-position buffer-end-position) ; forward
                (stmt-fetch-2 statement +number-of-buffered-rows+
                              oci:+fetch-absolute+ (1+ current-position)) ; OCI 1-based indexing
                (setf buffer-start-position current-position
                      buffer-end-position (min (+ current-position +number-of-buffered-rows+)
                                               row-count)))
               ((< current-position buffer-start-position) ; backward
                (stmt-fetch-2 statement +number-of-buffered-rows+ oci:+fetch-absolute+
                              (1+ (max (- (1+ current-position) +number-of-buffered-rows+) 0))) ; 1-based
                (setf buffer-start-position (max (- (1+ current-position) +number-of-buffered-rows+) 0)
                      buffer-end-position (1+ current-position)))
               (t
                #t)))))

(def function fetch-column-value (column-descriptor row-index)
  (rdbms.debug "Fetching ~S from buffer at index ~D" (name-of column-descriptor) row-index)
  (aprog1 (let* ((indicator (cffi:mem-aref (indicators-of column-descriptor) :short row-index)))
            (if (= indicator -1)
                :null
                (let* ((buffer (buffer-of column-descriptor))
                       (size (size-of column-descriptor))
                       (converter (typemap-oci-to-lisp (typemap-of column-descriptor))))
                  #+nil
                  (rdbms.dribble "Buffer:~%~A"
                                 (dump-c-byte-array buffer (* size +number-of-buffered-rows+)))
                  (rdbms.dribble "Convert from ~D, size is ~D, content:~%~A"
                                 (typemap-external-type (typemap-of column-descriptor)) size
                                 (dump-c-byte-array (cffi:inc-pointer buffer (* row-index size))
                                                    size))

                  (funcall converter
                           (cffi:inc-pointer buffer (* row-index size))
                           size))))
    (rdbms.debug "Fetched: ~S" it)))

(def method backend-release-savepoint (name (db oracle))) ;; TODO THL nothing needed?

(def method backend-type ((db oracle)) :oracle)
