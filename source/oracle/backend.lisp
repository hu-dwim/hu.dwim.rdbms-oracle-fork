;;; -*- mode: Lisp; Syntax: Common-Lisp; -*-
;;;
;;; Copyright (c) 2006 by the authors.
;;;
;;; See LICENCE for details.

(in-package :hu.dwim.rdbms.oracle)

(defconstant OCI_ATTR_DEFAULT_LOBPREFETCH_SIZE 438)
(defconstant OCI_ATTR_LOBPREFETCH_SIZE 439)
(defconstant OCI_ATTR_LOBPREFETCH_LENGTH 440)

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

(defun call-with-binder-buffer (is-null bval btype typemap fn)
  ;; TODO THL use typemap constructor and destructor?
  ;; TODO THL use cffi:with- for ptr?
  (multiple-value-bind (ptr size)
      (if is-null
          (if (lob-type-p btype)
              (make-lob-locator t) ;; TODO THL why needed when indicator is -1?
              (values null 0))
          (funcall (typemap-lisp-to-oci typemap) bval))
    (cond
      ((cffi-sys:null-pointer-p ptr)
       (funcall fn ptr size))
      ((lob-type-p btype)
       ;; TODO THL why &&locator and not &locator? stmt-execute crashes:-{
       (with-initialized-foreign-object (ptr2 :pointer ptr)
         (unwind-protect (funcall fn ptr2 (cffi:foreign-type-size :pointer))
           ;; TODO THL why freeing the locator crashes, double free?
           #+nil(cffi:foreign-free ptr))))
      (t
       (unwind-protect (funcall fn ptr size)
         (cffi:foreign-free ptr))))))

(defmacro with-binder-buffer ((ptr nbytes is-null bval btype typemap) &body body)
  `(call-with-binder-buffer ,is-null ,bval ,btype ,typemap
                            (lambda (,ptr ,nbytes) ,@body)))

(defun call-with-binder (stm tx pos1 btype bval fn)
  (let ((is-null (or (eql bval :null)
                     (and (not bval) (not (typep btype 'sql-boolean-type))))))
    (with-initialized-foreign-object (indicator 'oci:sb-2 (if is-null -1 0))
      (let ((typemap (typemap-for-sql-type btype)))
        (with-binder-buffer (ptr nbytes is-null bval btype typemap)
          (with-initialized-foreign-object (handle :pointer (cffi-sys:null-pointer))
            (oci-call (oci:bind-by-pos (statement-handle-of stm)
                                       handle
                                       (error-handle-of tx)
                                       pos1
                                       ptr
                                       nbytes
                                       (typemap-external-type typemap)
                                       indicator
                                       null ; alenp
                                       null ; rcodep
                                       0    ; maxarr_len
                                       null ; curelep
                                       *default-oci-flags*))
            (prog1 (funcall fn)
              (when (and (lob-type-p btype)
                         (not (member bval '(:null nil #()) :test #'equalp)))
                (upload-lob (cffi:mem-aref ptr :pointer) bval)))))))))

(defmacro with-binder ((stm tx pos1 btype bval) &body body)
  `(call-with-binder ,stm ,tx ,pos1 ,btype ,bval (lambda () ,@body)))

(defun call-with-binders (stm tx btypes bvals fn)
  (let ((n (length btypes)))
    (assert (eql n (length bvals)))
    (labels ((rec (i)
               (if (< i n)
                   (with-binder (stm tx (1+ i) (aref btypes i) (aref bvals i))
                     (rec (1+ i)))
                   (funcall fn))))
      (rec 0))))

(defmacro with-binders ((stm tx btypes bvals) &body body)
  `(call-with-binders ,stm ,tx ,btypes ,bvals (lambda () ,@body)))

;; use DEFIN3R, DEFINER clashes with hu.dwim.def:-{
(defstruct (defin3r
             (:constructor make-defin3r (indicators values value-size typemap)))
  indicators values value-size typemap)

(defun fetch-column-value (defin3r index)
  (with-slots (indicators values value-size typemap) defin3r
    (if (= -1 (cffi:mem-aref indicators :short index))
        :null
        (funcall (typemap-oci-to-lisp typemap)
                 (cffi:inc-pointer values (* index value-size))
                 value-size))))

(defun decode-row (defin3rs result-type)
  (ecase result-type
    (list
     (loop
        for d across defin3rs
        collect (fetch-column-value d 0)))
    (vector
     (loop
        with vals = (make-array (length defin3rs))
        for d across defin3rs
        for i from 0
        do (setf (aref vals i) (fetch-column-value d 0))
        finally (return vals)))))

(defun call-with-defin3r-buffer (nrows nbytes1 typemap fn)
  (let ((nbytes (* nrows nbytes1)))
    (cffi:with-foreign-object (ptr :uint8 nbytes)
      (dotimes (i nbytes)
        (setf (cffi:mem-ref ptr :uint8 i) 0))
      (let ((constructor (typemap-allocate-instance typemap)))
        (when constructor
          (assert (eql nbytes1 (cffi:foreign-type-size :pointer)))
          (dotimes (i nrows)
            (funcall constructor (cffi:inc-pointer ptr (* nbytes1 i))))))
      (unwind-protect (funcall fn ptr nbytes)
        (let ((destructor (typemap-free-instance typemap)))
          (when destructor
            (assert (eql nbytes1 (cffi:foreign-type-size :pointer)))
            (dotimes (i nrows)
              (funcall destructor (cffi:mem-ref ptr :pointer i)))))))))

(defmacro with-defin3r-buffer ((ptr nbytes nrows nbytes1 typemap) &body body)
  `(call-with-defin3r-buffer ,nrows ,nbytes1 ,typemap
                             (lambda (,ptr ,nbytes) ,@body)))

(defun call-with-defin3r (stm tx pos1 paraminfo fn &aux (nrows 1)) ;; TODO nrows
  (multiple-value-bind (ctype csize precision scale)
      (parse-paraminfo paraminfo)
    (let* ((typemap (typemap-for-internal-type ctype csize
                                               :precision precision
                                               :scale scale))
           (external-type (typemap-external-type typemap))
           (nbytes1 (data-size-for external-type csize)))
      (cffi:with-foreign-object (return-codes :unsigned-short nrows)
        (cffi:with-foreign-object (indicators :short nrows)
          (with-defin3r-buffer (ptr nbytes nrows nbytes1 typemap)
            (with-initialized-foreign-object (handle :pointer (cffi-sys:null-pointer))
              (oci:define-by-pos
                  (statement-handle-of stm)
                  handle
                (error-handle-of tx)
                pos1
                ptr
                nbytes
                external-type
                indicators
                null
                return-codes
                *default-oci-flags*)
              (funcall fn (make-defin3r indicators ptr nbytes1 typemap)))))))))

(defmacro with-defin3r ((var stm tx pos1 paraminfo) &body body)
  `(call-with-defin3r ,stm ,tx ,pos1 ,paraminfo (lambda (,var) ,@body)))

(defun call-with-defin3rs (stm tx fn)
  (let ((d (make-array 8 :adjustable t :fill-pointer 0)))
    (cffi:with-foreign-object (paraminfo :pointer)
      (labels ((rec (i)
                 (if (eql oci:+success+
                          (oci:param-get (statement-handle-of stm)
                                         oci:+htype-stmt+
                                         (error-handle-of tx)
                                         paraminfo
                                         (1+ i)))
                     (unwind-protect
                          (with-defin3r (x stm tx (1+ i)
                                           (cffi:mem-ref paraminfo :pointer))
                            (vector-push-extend x d)
                            (rec (1+ i)))
                       (oci:descriptor-free paraminfo oci:+dtype-param+))
                     (funcall fn d))))
        (rec 0)))))

(defmacro with-defin3rs ((var stm tx) &body body)
  `(call-with-defin3rs ,stm ,tx (lambda (,var) ,@body)))

(defun make-list-row-visitor ()
  (let ((x (cons nil nil)))
    (setf (car x) x)
    (lambda (row)
      (let ((y (cons row nil)))
        (setf (cdar x) y
              (car x) y))
      (cdr x))))

(defun make-vector-row-visitor ()
  (let ((x (make-array 8 :adjustable t :fill-pointer 0)))
    (lambda (row)
      (vector-push-extend row x)
      x)))

(defun make-row-visitor (custom-visitor result-type) ;; TODO THL remove this, apps should always specify visitor
  (or custom-visitor
      (ecase result-type
        (list (make-list-row-visitor))
        (vector (make-vector-row-visitor)))))

(defun execute-prepared-statement (transaction statement binding-types binding-values visitor result-type)
  ;; execute
  (with-binders (statement transaction binding-types binding-values)
    (stmt-execute statement *default-oci-flags*))
  ;; fetch
  (cond
    ((select-p statement)
     (with-defin3rs (d statement transaction)
       (loop
          with z = nil
          with xvisitor = (make-row-visitor visitor result-type)
          while (when (stmt-fetch-2 statement 1 oci:+fetch-next+ 0)
                  (assert (eql 1 (get-statement-attribute
                                  statement
                                  oci:+attr-rows-fetched+
                                  'oci:ub-4)))
                  t)
          do (setq z (funcall xvisitor (decode-row d result-type)))
          finally (return (values z (get-row-count-attribute statement))))))
    (t
     (values nil (get-row-count-attribute statement))))) ;; TODO THL what should the first value be?

(defun parse-paraminfo (paraminfo)
  (cffi:with-foreign-objects ((attribute-value :uint8 8)
                              (attribute-value-length 'oci:ub-4))
    (macrolet
	;; use a macro so that the compiler macro on mem-ref can see the
	;; cffi-type!  essential for speed.
	((%oci-attr-get (attribute-id cffi-type)
	   `(progn
	      (oci-attr-get paraminfo ,attribute-id attribute-value attribute-value-length)
	      (cffi:mem-ref attribute-value ,cffi-type)))
	 (oci-string-attr-get (attribute-id)
	   `(progn
	      (oci-attr-get paraminfo ,attribute-id attribute-value attribute-value-length)
	      (oci-string-to-lisp
	       (cffi:mem-ref attribute-value :pointer) ; OraText*
	       (cffi:mem-ref attribute-value-length 'oci:ub-4)))))
      (let ((column-type (%oci-attr-get oci:+attr-data-type+ 'oci:ub-2))
	    (column-size)
	    (precision)
	    (scale))
	(declare (fixnum column-type))
	(progn
	  ;; KLUDGE oci:+attr-data-size+ returned as ub-2, despite it is documented as ub-4
	  (setf (cffi:mem-ref attribute-value :unsigned-short) 0)
	  (setf column-size (%oci-attr-get oci:+attr-data-size+ 'oci:ub-2)))
	(when (= column-type oci:+sqlt-num+)
	  ;; the type of the precision attribute is 'oci:sb-2, because we
	  ;; use an implicit describe here (would be sb-1 for explicit describe)
	  (setf precision (%oci-attr-get oci:+attr-precision+ 'oci:sb-2)
		scale (%oci-attr-get oci:+attr-scale+ 'oci:sb-1)))
	(values column-type column-size precision scale)))))

(def method backend-release-savepoint (name (db oracle))) ;; TODO THL nothing needed?

(def method backend-type ((db oracle)) :oracle)
