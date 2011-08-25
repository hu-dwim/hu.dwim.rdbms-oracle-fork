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
  (cffi:foreign-free (statement-handle-pointer statement)))

(defmacro with-initialized-foreign-object ((var type value) &body body)
  `(cffi:with-foreign-object (,var ,type)
     (setf (cffi:mem-ref ,var ,type) ,value)
     ,@body))

(defun call-with-binding (stm tx pos btype bval fn)
  (let ((is-null (or (eql bval :null)
                     (and (not bval) (not (typep btype 'sql-boolean-type))))))
    (with-initialized-foreign-object (&indicator 'oci:sb-2 (if is-null -1 0))
      (let ((typemap (typemap-for-sql-type btype)))
        (multiple-value-bind (&data size)
            (if is-null
                (if (lob-type-p btype)
                    (make-lob-locator t) ;; TODO THL why needed when indicator is -1?
                    (values null 0))
                (funcall (typemap-lisp-to-oci typemap) bval))
          ;; TODO THL why &&locator and not &locator? stmt-execute crashes:-{
          (when (lob-type-p btype)
            (setq &data (foreign-alloc-with-initial-element :pointer :initial-element &data)))
          (unwind-protect
               (cffi:with-foreign-object (&&bind-handle :pointer)
                 (oci-call (oci:bind-by-pos (statement-handle-of stm)
                                            &&bind-handle
                                            (error-handle-of tx)
                                            pos
                                            &data
                                            size
                                            (typemap-external-type typemap)
                                            &indicator
                                            null ; alenp
                                            null ; rcodep
                                            0    ; maxarr_len
                                            null ; curelep
                                            *default-oci-flags*))
                 (funcall fn)
                 (when (and (lob-type-p btype)
                            (not (member bval '(:null nil #()) :test #'equalp)))
                   (upload-lob (cffi:mem-aref &data :pointer) bval)))
            (unless (cffi:null-pointer-p &data)
              ;; TODO THL free locator inside too if lob-type-p?
              (cffi:foreign-free &data))))))))

(defmacro with-binding ((stm tx pos btype bval) &body body)
  `(call-with-binding ,stm ,tx ,pos ,btype ,bval (lambda () ,@body)))

(defun call-with-bindings (stm tx btypes bvals fn)
  (let ((n (length btypes)))
    (assert (eql n (length bvals)))
    (labels ((rec (i)
               (if (< i n)
                   (with-binding (stm tx (1+ i) (aref btypes i) (aref bvals i))
                     (rec (1+ i)))
                   (funcall fn))))
      (rec 0))))

(defmacro with-bindings ((stm tx btypes bvals) &body body)
  `(call-with-bindings ,stm ,tx ,btypes ,bvals (lambda () ,@body)))

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
    ;; TODO THL configurable prefetching limits?
    (set-statement-attribute statement oci:+attr-prefetch-rows+ 1000000)
    (set-statement-attribute statement oci:+attr-prefetch-memory+ #. (* 10 (expt 2 20)))

    ;; execute
    (with-bindings (statement transaction binding-types binding-values)
      (stmt-execute statement *default-oci-flags*))

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
       (values nil (get-row-count-attribute statement)))))) ;; TODO THL what should the first value be?

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
