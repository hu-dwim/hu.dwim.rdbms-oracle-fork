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
  (t (:default "libclntsh")))

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
                            &key visitor binding-types binding-values result-type out-position
                            &allow-other-keys)
  (rdbms.debug "Executing ~S" command)
  (with-falloc ()
    (let ((statement (prepare-command database transaction command)))
      (unwind-protect
           (execute-prepared-statement transaction statement binding-types binding-values visitor result-type out-position)
        (free-prepared-statement statement)))))

(def method execute-command ((database oracle)
                            (transaction oracle-transaction)
                            (prepared-statement prepared-statement)
                            &key visitor binding-types binding-values result-type out-position
                            &allow-other-keys)
  (execute-prepared-statement transaction prepared-statement binding-types binding-values visitor result-type out-position))

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

(defun set-default-lob-prefetching (value)
  (set-session-attribute OCI_ATTR_DEFAULT_LOBPREFETCH_SIZE value))

(defun logon (tx env datasource username password)
  (rdbms.debug "Logging on in transaction ~A" tx)
  (if *use-connection-pool*
      (server-attach-using-pool (ensure-connection-pool
                                 env
                                 datasource
                                 username
                                 password))
      (server-attach datasource))
  (oci-call (oci:attr-set (service-context-handle-of tx)
                          oci:+htype-svcctx+
                          (server-handle-of tx)
                          0
                          oci:+attr-server+
                          (error-handle-of tx)))
  (handle-alloc (session-handle-pointer tx) oci:+htype-session+)
  (set-session-string-attribute oci:+attr-username+ username)
  (set-session-string-attribute oci:+attr-password+ password)
  (oci-call (oci:session-begin (service-context-handle-of tx)
                               (error-handle-of tx)
                               (session-handle-of tx)
                               oci:+cred-rdbms+
                               oci:+default+))
  (oci-call (oci:attr-set (service-context-handle-of tx)
                          oci:+htype-svcctx+
                          (session-handle-of tx)
                          0
                          oci:+attr-session+
                          (error-handle-of tx))))

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
            (logon transaction environment datasource user-name password)
            (return-from connecting))
          (unless (cffi:null-pointer-p (session-handle-of transaction))
            (oci-call (oci:handle-free (session-handle-of transaction) oci:+htype-session+))
            (setf (session-handle-of transaction) null)))
    ;;(set-default-lob-prefetching #.(* 1024 1024))
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

;;; When executing a prepared statement, there are:
;;;
;;; - bind in parameters
;;; - bind out parameters
;;; - definers
;;;
;;; Binders represent the query parameters that are passed along and
;;; marked in the query like :1 etc.  These can be "in" (where simple
;;; data is simply passed directly to the server) or "out" (query
;;; parameters after the RETURNING sql clause).  The out parameters
;;; allow for lob uploading after an INSERT/UPDATE/DELETE query has
;;; been executed at which point the server sent back appropriate lob
;;; locators.  It doesn't seem to be possible to upload lobs via in
;;; bind parameters.  The bind out parameters must use dynamic binding
;;; (OCI_DATA_AT_EXEC mode) and use callbacks if more then one row is
;;; returned, e.g. during nbatch (bulk insert/update).
;;;
;;; Definers represent results of the query as specified in the query
;;; select-list.  To reduce the number of round-trips to the server,
;;; several rows are fetched at once.  Lobs for those rows are fetched
;;; all in one go after that.

;;; binders

(defun alloc-indicator (is-null)
  (heap-falloc 'oci:sb-2 1 (if is-null -1 0)))

(defun is-null-p (bval btype)
  (or (eql bval :null)
      (and (cl:null bval)
           (not (typep btype 'sql-boolean-type)))))

(defun convert-value (bval btype out-variable-p)
  (cond
    ((consp bval) ;; nbatch, data allocated dynamically later
     (values null #.(* 1024 1024) null)) ;; feel free to use other max size
    ((and out-variable-p (lob-type-p btype))
     (multiple-value-bind (ptr len) (make-lob-locator-indirect (equalp #() bval))
       (values ptr len (alloc-indicator (is-null-p bval btype)))))
    ((is-null-p bval btype)
     (values null 0 (alloc-indicator t)))
    (t
     (multiple-value-bind (ptr len)
         (funcall (typemap-lisp-to-oci (typemap-for-sql-type btype)) bval)
       (values ptr len (alloc-indicator nil))))))

;;; nbatch stuff

(defvar *first-out-position*) ;; 0-based (vs 1-based binding position)

;; TODO THL better then passing these through callback pointers?
(defvar *dynamic-binding-values*)
(defvar *dynamic-binding-types*)
(defvar *dynamic-binding-locators*)

(defvar *convert-value-alloc-lob*)

(defun free-later-lob (x)
  (push x *convert-value-alloc-lob*)
  x)

(defun out-position-p (pos0)
  (when *first-out-position*
    (<= *first-out-position* pos0)))

(defun bind-dynamic-in (ictxp bindp iter index bufpp alenp piecep indpp)
  (declare (ignore bindp index))
  (let* ((pos0 (cffi-sys:pointer-address ictxp))
         (value (nth iter (elt *dynamic-binding-values* pos0)))
         (sql-type (elt *dynamic-binding-types* pos0)))
    (multiple-value-bind (ptr len ind)
        (convert-value value sql-type (out-position-p pos0))
      (if (and (lob-type-p sql-type)
               (not (cffi-sys:null-pointer-p ptr)))
          (let ((locator (cffi:mem-ref ptr :pointer)))
            (free-later-lob locator)
            (setf (cffi:mem-ref bufpp :pointer) locator
                  (cffi:mem-ref alenp 'oci:ub-4) len
                  (cffi:mem-ref piecep 'oci:ub-1) oci:+one-piece+
                  (cffi:mem-ref indpp :pointer) ind))
          (setf (cffi:mem-ref bufpp :pointer) ptr
                (cffi:mem-ref alenp 'oci:ub-4) len
                (cffi:mem-ref piecep 'oci:ub-1) oci:+one-piece+
                (cffi:mem-ref indpp :pointer) ind))))
  oci:+continue+)

(cffi:defcallback bind-dynamic-in-cb oci:sb-4 ((ictxp :pointer) ;; dvoid*
                                               (bindp :pointer) ;; OCIBind*
                                               (iter oci:ub-4)
                                               (index oci:ub-4)
                                               (bufpp :pointer) ;; dvoid**
                                               (alenp :pointer) ;; ub4*
                                               (piecep :pointer) ;; ub1*
                                               (indpp :pointer)) ;; dvoid**
  (bind-dynamic-in ictxp bindp iter index bufpp alenp piecep indpp))

(defun bind-dynamic-out (octxp bindp iter index bufpp alenp piecep indpp rcodepp)
  (declare (ignore bindp index))
  (let* ((pos0 (cffi-sys:pointer-address octxp))
         (value (nth iter (elt *dynamic-binding-values* pos0)))
         (sql-type (elt *dynamic-binding-types* pos0)))
    (assert (lob-type-p sql-type)) ;; TODO THL do not crash foreign code;-)
    (multiple-value-bind (ptr len ind)
        (convert-value value sql-type t)
      (let ((locator (cffi:mem-ref ptr :pointer)))
        (free-later-lob locator)
        (push locator (aref *dynamic-binding-locators* pos0))
        (setf (cffi:mem-ref bufpp :pointer) locator
              (cffi:mem-ref (cffi:mem-ref alenp :pointer) 'oci:ub-4) len
              (cffi:mem-ref piecep 'oci:ub-1) oci:+one-piece+
              (cffi:mem-ref indpp :pointer) ind
              (cffi:mem-ref rcodepp :pointer) null))))
  oci:+continue+)

(cffi:defcallback bind-dynamic-out-cb oci:sb-4 ((octxp :pointer) ;; dvoid*
                                                (bindp :pointer) ;; OCIBind*
                                                (iter oci:ub-4)
                                                (index oci:ub-4)
                                                (bufpp :pointer) ;; dvoid**
                                                (alenp :pointer) ;; ub4**
                                                (piecep :pointer) ;; ub1*
                                                (indpp :pointer) ;; dvoid**
                                                (rcodepp :pointer)) ;; ub2**
  (bind-dynamic-out octxp bindp iter index bufpp alenp piecep indpp rcodepp))

;;; more binders

(defun null-or-empty-value-p (x)
  (member x '(:null nil #()) :test #'equalp))

(defun guess-first-out-position (stm btype pos1)
  (when (and (or (insert-p stm) (update-p stm))
             (lob-type-p btype)
             (not *first-out-position*))
    ;; Ideally, compiling rdbms queries should return out-position
    ;; alongside binding values and types and this information should
    ;; be then passed to execute-command by perec. For now, I rely on
    ;; the fact that lob bindings are always in the out position when
    ;; insert/update queries are compiled using rdbms and run by
    ;; perec.
    (setq *first-out-position* (1- pos1))))

(defun call-with-binder (stm tx pos1 btype bval nbatch fn) ;; TODO THL delete-p for out binders?
  (guess-first-out-position stm btype pos1)
  (let* ((typemap (typemap-for-sql-type btype))
         (pos0 (1- pos1))
         (out-position-p (out-position-p pos0)))
    ;; TODO THL use typemap constructor and destructor?
    ;; TODO THL use cffi:with- for ptr? instead of alloc and free?
    (multiple-value-bind (ptr len ind)
        (convert-value bval btype out-position-p)
      (with-falloc-object (handle :pointer 1 (cffi-sys:null-pointer))
        (oci-call (oci:bind-by-pos (statement-handle-of stm)
                                   handle
                                   (error-handle-of tx)
                                   pos1
                                   ptr
                                   len
                                   (typemap-external-type typemap)
                                   ind
                                   null ; alenp
                                   null ; rcodep
                                   0    ; maxarr_len
                                   null ; curelep
                                   (if nbatch
                                       oci:+data-at-exec+
                                       *default-oci-flags*)))
        (cond
          (nbatch
           (oci-call (oci:bind-dynamic (cffi:mem-ref handle :pointer)
                                       (error-handle-of tx)
                                       (cffi-sys:make-pointer pos0)
                                       (cffi:callback bind-dynamic-in-cb)
                                       (cffi-sys:make-pointer pos0)
                                       (cffi:callback bind-dynamic-out-cb))))
          (t
           (when (and (lob-type-p btype)
                      (not (cffi-sys:null-pointer-p ptr)))
             (free-later-lob (cffi:mem-ref ptr :pointer)))))
        (prog1 (funcall fn)
          (when (and out-position-p (lob-type-p btype))
            (cond
              (nbatch
               (loop
                  for x in bval
                  for locator in (nreverse (aref *dynamic-binding-locators* pos0))
                  when (and locator (not (null-or-empty-value-p x)))
                  do (upload-lob locator x)))
              ((not (null-or-empty-value-p bval))
               (upload-lob (cffi:mem-aref ptr :pointer) bval)))))))))

(defmacro with-binder ((stm tx pos1 btype bval nbatch) &body body)
  `(call-with-binder ,stm ,tx ,pos1 ,btype ,bval ,nbatch (lambda () ,@body)))

(defun call-with-binders (stm tx btypes bvals nbatch fn)
  (let ((n (length btypes)))
    (assert (eql n (length bvals)))
    (labels ((rec (i)
               (if (< i n)
                   (with-binder (stm tx (1+ i) (aref btypes i) (aref bvals i) nbatch)
                     (rec (1+ i)))
                   (funcall fn))))
      (rec 0))))

(defmacro with-binders ((stm tx btypes bvals nbatch) &body body)
  `(call-with-binders ,stm ,tx ,btypes ,bvals ,nbatch (lambda () ,@body)))

;;; defin3rs

;; use DEFIN3R, DEFINER clashes with hu.dwim.def:-{
(defstruct (defin3r
             (:constructor make-defin3r (indicators values value-size typemap lobv)))
  indicators values value-size typemap lobv)

#+nil
(cffi:defcfun ("OCIArrayDescriptorAlloc" OCIArrayDescriptorAlloc) oci:sword
  (parenth :pointer)
  (descpp :pointer)
  (type oci:ub-4)
  (array_size oci:ub-4)
  (xtramem_sz oci:size-t)
  (usrmempp :pointer))

#+nil
(cffi:defcfun ("OCIArrayDescriptorFree" OCIArrayDescriptorFree) oci:sword
  (descp :pointer)
  (type oci:ub-4))

(defun alloc-descriptors (descpp n dtype)
  (declare (optimize speed)
           (type fixnum n))
  #+nil
  (oci-call (OCIArrayDescriptorAlloc (environment-handle-of *transaction*)
                                     descpp
                                     dtype
                                     nrows1
                                     0
                                     (cffi-sys:null-pointer)))
  #+nil
  (dotimes (i nrows1)
    (oci-call (oci:descriptor-alloc (environment-handle-of *transaction*)
                                    (cffi:inc-pointer descpp (* nbytes1 i))
                                    dtype
                                    0
                                    (cffi-sys:null-pointer))))
  (let ((envh (environment-handle-of *transaction*))
        (offset 0))
    (declare (type fixnum offset))
    (dotimes (i n)
      ;;(declare (type fixnum i))
      (oci-call (oci:descriptor-alloc envh
                                      (cffi:inc-pointer descpp offset)
                                      dtype
                                      0
                                      (cffi-sys:null-pointer)))
      (incf offset #.(cffi:foreign-type-size :pointer))))
  #+nil
  (loop
     with envh = (environment-handle-of *transaction*)
     for i from 0 below n
     for offset from 0 by #.(cffi:foreign-type-size :pointer)
     do (oci-call (oci:descriptor-alloc envh
                                        (cffi:inc-pointer descpp offset)
                                        dtype
                                        0
                                        (cffi-sys:null-pointer)))))

(defun free-descriptors (descpp n dtype)
  (declare (optimize speed)
           (type fixnum n))
  #+nil
  (oci-call (OCIArrayDescriptorFree descpp dtype))
  (let ((offset 0))
    (declare (type fixnum offset))
    (dotimes (i n)
      ;;(declare (type fixnum i))
      (oci-call
       (oci:descriptor-free #-allegro(cffi:mem-aref descpp :pointer i)
                            #+allegro(system:memref-int descpp offset 0 :unsigned-long)
                            dtype))
      (incf offset #.(cffi:foreign-type-size :pointer)))))

(defun call-with-defin3r-buffer (nrows1 nbytes1 typemap fn)
  (let ((nbytes (* nrows1 nbytes1))
        (dtype (case (typemap-external-type typemap)
                 (112 oci:+dtype-lob+)
                 (113 oci:+dtype-lob+)
                 (187 oci:+dtype-timestamp+)
                 (188 oci:+dtype-timestamp-tz+))))
    (with-falloc-object (ptr :uint8 nbytes 0)
      (when dtype
        (assert (eql #.(cffi:foreign-type-size :pointer) nbytes1))
        (alloc-descriptors ptr nrows1 dtype))
      (unwind-protect (funcall fn ptr nbytes)
        (when dtype
          (free-descriptors ptr nrows1 dtype))))))

(defmacro with-defin3r-buffer ((ptr nbytes nrows1 nbytes1 typemap) &body body)
  `(call-with-defin3r-buffer ,nrows1 ,nbytes1 ,typemap
                             (lambda (,ptr ,nbytes) ,@body)))

(defun parse-paraminfo (paraminfo)
  (with-falloc-objects ((attribute-value :uint8 8)
                        (attribute-value-length oci:ub-4))
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

(defun set-lob-prefetching (defin3r-handle size length)
  (when size
    (set-attribute defin3r-handle oci:+htype-define+ OCI_ATTR_LOBPREFETCH_SIZE
                   size))
  (when length
    (set-attribute defin3r-handle oci:+htype-define+ OCI_ATTR_LOBPREFETCH_LENGTH
                   length)))

(defun call-with-defin3r (tx stm nrows1 pos1 paraminfo fn)
  (multiple-value-bind (ctype csize precision scale)
      (parse-paraminfo paraminfo)
    (let* ((typemap (typemap-for-internal-type ctype csize
                                               :precision precision
                                               :scale scale))
           (external-type (typemap-external-type typemap))
           (nbytes1 (data-size-for external-type csize)))
      (with-falloc-objects ((return-codes :unsigned-short nrows1)
                            (indicators :short nrows1))
        (with-defin3r-buffer (ptr nbytes nrows1 nbytes1 typemap)
          (declare (ignore nbytes))
          (with-falloc-object (handle :pointer 1 (cffi-sys:null-pointer))
            (oci:define-by-pos
                (statement-handle-of stm)
                handle
              (error-handle-of tx)
              pos1
              ptr
              nbytes1
              external-type
              indicators
              null
              return-codes
              *default-oci-flags*))
          (funcall fn (make-defin3r indicators ptr nbytes1 typemap
                                    (case external-type
                                      ((112 113)
                                       (make-array nrows1))))))))))

(defmacro with-defin3r ((var tx stm nrows1 pos1 paraminfo) &body body)
  `(call-with-defin3r ,tx ,stm ,nrows1 ,pos1 ,paraminfo (lambda (,var) ,@body)))

(defun call-with-defin3rs (tx stm nrows1 fn)
  (let ((d (make-array 8 :adjustable t :fill-pointer 0)))
    (labels ((rec (i)
               (with-falloc-object (paraminfo :pointer)
                 (if (eql oci:+success+
                          (oci:param-get (statement-handle-of stm)
                                         oci:+htype-stmt+
                                         (error-handle-of tx)
                                         paraminfo
                                         (1+ i)))
                     (unwind-protect
                          (with-defin3r (x tx stm nrows1 (1+ i)
                                           (cffi:mem-ref paraminfo :pointer))
                            (vector-push-extend x d)
                            (rec (1+ i)))
                       (oci:descriptor-free paraminfo oci:+dtype-param+))
                     (funcall fn d)))))
      (rec 0))))

(defmacro with-defin3rs ((var tx stm nrows1) &body body)
  `(call-with-defin3rs ,tx ,stm ,nrows1 (lambda (,var) ,@body)))

;;; prepared statement execution

(defun set-row-prefetching (stm rows-limit memory-limit)
  (when rows-limit
    (set-statement-attribute stm oci:+attr-prefetch-rows+ rows-limit))
  (when memory-limit
    (set-statement-attribute stm oci:+attr-prefetch-memory+ memory-limit)))

(defmacro with-list-appender (&body body)
  (let ((x (gensym)))
    `(let ((,x (cons nil nil)))
       (setf (car ,x) ,x)
       (macrolet ((appending (&body body)
                    (let ((y (gensym)))
                      `(let ((,y (cons (progn ,@body) nil)))
                         (setf (cdar ,',x) ,y
                               (car ,',x) ,y))))
                  (appended ()
                    `(cdr ,',x))
                  (reset ()
                    `(setf ,',x (cons nil nil)
                           (car ,',x) ,',x)))
         ,@body))))

(defmacro with-vector-appender (&body body)
  (let ((v (gensym)))
    `(let ((,v (make-array 8 :adjustable t :fill-pointer 0)))
       (macrolet ((appending (&body body)
                    `(vector-push-extend (progn ,@body) ,',v))
                  (appended ()
                    `(progn ,',v))
                  (reset ()
                    `(setf (fill-pointer ,',v) 0)))
         ,@body))))

(defmacro with-appender (type &body body)
  `(ecase ,type
     (list (with-list-appender ,@body))
     (vector (with-vector-appender ,@body))))

(defun make-list-collector ()
  (with-list-appender (lambda (x) (appending x) (appended))))

(defun make-vector-collector ()
  (with-vector-appender (lambda (x) (appending x) (appended))))

(defun make-collector (custom-visitor result-type) ;; TODO THL remove this, apps should always specify visitor, both for cols and rows?
  (or custom-visitor
      (ecase result-type
        (list (make-list-collector))
        (vector (make-vector-collector)))))

(defun attr-rows-fetched (stm)
  (get-statement-attribute stm oci:+attr-rows-fetched+ oci:ub-4))

(defun attr-row-count (stm)
  (get-statement-attribute stm oci:+attr-row-count+ oci:ub-4))

(defun %decode-value (bufp alen ind typemap)
  (ecase ind
    (-1 :null)
    (0 (ecase (typemap-external-type typemap)
         (#.oci:+sqlt-timestamp+ (local-time-from-timestamp bufp alen) #+nil(decode-datetime bufp))
         (#.oci:+sqlt-timestamp-tz+ (local-time-from-timestamp-tz bufp alen) #+nil(decode-datetime-tz bufp))
         (#.oci:+sqlt-str+ (oci-string-to-lisp bufp alen))
         (#.oci:+sqlt-vnu+ (rational-from-varnum bufp alen))
         (#.oci:+sqlt-afc+ (boolean-from-char bufp alen))
         ;;(#.oci:+sqlt-int+ (error "hi1"))
         ;;(#.oci:+sqlt-bfloat+ (error "hi2"))
         (#.oci:+sqlt-bdouble+
          (double-from-bdouble bufp alen)
          #+nil
          (let ((*read-eval* nil)
                (*read-default-float-format* 'double-float))
            (coerce (read-from-string (oci-string-to-lisp bufp alen)) 'double-float)))
         (#.oci:+sqlt-dat+
          (cdate-from-date bufp alen)
          #+nil
          (multiple-value-bind (y m d) (decode-date bufp)
            (make-cdate y m d)))
         ;;(#.oci:+sqlt-odt+ (error "hi3") (cdate-from-date bufp alen))
         ))))

;; (defun decode-value (bufp nbytes alenp indp typemap) ;; TODO THL s/typemap/external-type
;;   (let ((alen (cffi:mem-ref alenp 'oci:ub-4))
;;         (ind (cffi:mem-ref indp 'oci:sb-2)))
;;     ;;(print (list :@@@ alen))
;;     (assert (< alen nbytes))
;;     ;; TODO THL assert rcode bellow?  (oci-call (cffi:mem-ref rcodep
;;     ;; 'oci:ub-2))
;;     ;; http://www.helsinki.fi/~atkk_klp/oradoc/DOC/api/doc/OCI73/ch4a.htm
;;     ;; Typical error codes would indicate that data in progv has been
;;     ;; truncated [ORA-01406] or that a null occurred on a SELECT or
;;     ;; PL/SQL FETCH [ORA-01405].  (print (list :@@@ alen ind))
;;     ;;(print (list :@@@ (typemap-external-type typemap)))
;;     (%decode-value bufp alen ind typemap)))

(defmacro zacross ((e vec) &body body)
  (let ((z (gensym)))
    `(loop
        with ,z = nil
        for ,e across ,vec
        do (setq ,z (locally ,@body))
        finally (return ,z))))

(defmacro zdotimes ((i n) &body body)
  (let ((z (gensym)))
    `(let (,z)
       (dotimes (,i ,n ,z)
         (setq ,z (locally ,@body))))))

(defun decode-cell (defin3r r)
  (with-struct (defin3r- indicators values value-size typemap lobv) defin3r
    (if lobv
        (aref lobv r)
        (%decode-value (cffi:inc-pointer values (* r value-size))
                       value-size
                       (cffi:mem-aref indicators :short r)
                       typemap))))

(defun decode-row (defin3rs r cfn)
  (zacross (d defin3rs)
    (funcall cfn (decode-cell d r))))

(defvar *download-lobs-buffer*)
(defvar *pending-lobs*)

(defun %download-lobs-cb (ctxp array-iter bufxp len #+allegro len-hi piece changed-bufpp changed-lenp)
  (declare (ignore ctxp array-iter changed-bufpp changed-lenp))
  #+allegro
  (setq len (+ (ash len-hi 32) len))
  (dotimes (i len)
    (vector-push-extend (cffi:mem-aref bufxp 'oci:ub-1 i) *download-lobs-buffer*))
  (ecase piece
    ((#.oci:+first-piece+ #.oci:+next-piece+))
    (#.oci:+last-piece+
     (funcall (cdr (pop *pending-lobs*)) *download-lobs-buffer*)
     (setf (fill-pointer *download-lobs-buffer*) 0)))
  oci:+continue+)

(cffi:defcallback download-lobs-cb oci:sb-4 ((ctxp :pointer)
                                             (array-iter oci:ub-4)
                                             (bufxp :pointer)
                                             #-allegro
                                             (len oci:oraub-8)
                                             #+allegro
                                             (len oci:ub-4)
                                             #+allegro
                                             (len-hi oci:ub-4)
                                             (piece oci:ub-1)
                                             (changed-bufpp :pointer)
                                             (changed-lenp :pointer))
  (%download-lobs-cb ctxp array-iter bufxp len #+allegro len-hi piece changed-bufpp changed-lenp))

(defun download-lobs-using-callback (locators n &optional csid)
  (let* ((svchp (service-context-handle-of *transaction*))
         (errhp (error-handle-of *transaction*))
         (siz #.(* 8 8 1024 #+nil(lob-chunk-size svchp errhp (cffi:mem-aref locators :pointer))))
         (*download-lobs-buffer* (make-array siz
                                             :element-type '(unsigned-byte 8)
                                             :adjustable t
                                             :fill-pointer 0)))
    (with-falloc-objects ((bamtp oci:oraub-8 n 0)
                          (camtp oci:oraub-8 n 0)
                          (offp oci:oraub-8 n 1)
                          (np oci:ub-4 1 n)
                          (buf oci:ub-1 siz)
                          (bufp :pointer n buf)
                          (bufl oci:oraub-8 n siz))
      (oci-call
       (oci:lob-array-read svchp errhp np locators
                           bamtp camtp offp bufp bufl
                           oci:+first-piece+
                           null (cffi:callback download-lobs-cb)
                           (or csid 0) oci:+sqlcs-implicit+)))))

(defun ensure-lobs-downloaded (defin3rs nrows)
  (with-list-appender
    (let ((n 0))
      (zacross (d defin3rs)
        (with-struct (defin3r- indicators values value-size typemap lobv) d
          (when lobv
            (assert (eql #.(cffi:foreign-type-size :pointer) value-size))
            (dotimes (r nrows)
              (let ((locator (cffi:mem-aref values :pointer r))
                    (indicator (cffi:mem-aref indicators :short r)))
                (ecase indicator
                  (-1
                   (setf (aref lobv r) :null))
                  (0
                   (appending
                    (cons locator
                          (let ((rr r)
                                (external-type (typemap-external-type typemap)))
                            (lambda (x)
                              (setf (aref lobv rr)
                                    (ecase external-type
                                      (112 ;; clob
                                       (babel:octets-to-string x :encoding :utf-16le))
                                      (113 ;; blob
                                       (coerce x '(simple-array (unsigned-byte 8) (*))))))))))
                   (incf n))))))))
      (let ((*pending-lobs* (appended)))
        (when *pending-lobs*
          (with-falloc-object (bufp :pointer n)
            (loop
               for (locator) in *pending-lobs*
               for i from 0
               do (setf (cffi:mem-aref bufp :pointer i) locator))
            (download-lobs-using-callback bufp n oci:+utf-16-id+)))))))

(defvar *nrows1* 42)

(defun fetch-rows (tx stm rfn mkcfn &optional (nrows1 *nrows1*))
  (with-defin3rs (d tx stm nrows1)
    (when (plusp (length d))
      (do (z
           (nrows 0))
          ((not (let ((more (stmt-fetch-2 stm nrows1 oci:+fetch-next+ 0))
                      (n (attr-rows-fetched stm)))
                  (when (plusp n)
                    (assert (<= n nrows1))
                    (incf nrows n)
                    (ensure-lobs-downloaded d n)
                    (setq z (zdotimes (r n)
                              (funcall rfn (decode-row d r (funcall mkcfn)))))
                    more)))
           (progn
             (assert (eql nrows (attr-row-count stm)))
             z))))))

(defun execute-prepared-statement (tx stm btypes bvalues visitor result-type out-position)
  ;; TODO THL configurable prefetching limits?
  ;;(set-row-prefetching stm 1000000 #.(* 10 (expt 2 20)))
  ;; execute
  (let* ((nbatch (when bvalues
                   (let ((u (remove-duplicates
                             (loop
                                for x across bvalues
                                collect (when (consp x) (length x))))))
                     (assert (<= 0 (length u) 1))
                     (when (car u)
                       (assert (numberp (car u)))
                       (car u)))))
         (*first-out-position* (and out-position (1- out-position)))
         (*dynamic-binding-values* bvalues)
         (*dynamic-binding-types* btypes)
         (*dynamic-binding-locators*
          (and nbatch (make-array (length btypes) :initial-element nil)))
         (*convert-value-alloc-lob* nil))
    (unwind-protect
         (with-binders (stm tx btypes bvalues nbatch)
           (stmt-execute stm *default-oci-flags* nbatch))
      (mapc 'free-oci-lob-locator *convert-value-alloc-lob*)))
  (values
   (fetch-rows tx stm
               (make-collector visitor result-type)
               (lambda () (make-collector nil result-type)))
   (attr-row-count stm)))

(def method backend-release-savepoint (name (db oracle))) ;; TODO THL nothing needed?

(def method backend-type ((db oracle)) :oracle)
