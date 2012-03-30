;;; -*- mode: Lisp; Syntax: Common-Lisp; -*-
;;;
;;; Copyright (c) 2006 by the authors.
;;;
;;; See LICENCE for details.

(in-package :hu.dwim.rdbms.oracle)

(def special-variable *default-oci-flags* (logior oci:+threaded+ oci:+new-length-semantics+))

;;; helpers to access foreign stuff

(def macro dereference-foreign-pointer (data-pointer type &optional size-pointer)
  (if (eq type :string)
      `(cffi:foreign-string-to-lisp
        (cffi:mem-ref ,data-pointer :pointer)
        :count (cffi:mem-ref ,size-pointer 'oci:ub-4)
        :encoding (connection-encoding-of (database-of *transaction*)))
      `(cffi:mem-ref ,data-pointer ',type)))

(def function oci-attr-get (param-descriptor
                     attribute-id
                     attribute-value         ; output
                     attribute-value-length) ; output
  (oci-call (oci:attr-get param-descriptor
                          oci:+dtype-param+
                          attribute-value
                          attribute-value-length
                          attribute-id
                          (error-handle-of *transaction*))))

(def macro get-param-descriptor-attribute (param-descriptor attribute type)
  `(with-falloc-objects ((data-pointer ,type)
                         (size-pointer oci:ub-4))
    (oci-call (oci:attr-get ,param-descriptor
               oci:+dtype-param+
               data-pointer
               size-pointer
               ,attribute
               (error-handle-of *transaction*)))
    (dereference-foreign-pointer data-pointer ,type size-pointer)))

(def macro get-statement-attribute (statement attribute type)
  `(with-falloc-objects ((data-pointer ,type)
                         (size-pointer oci:ub-4))
    (oci-call (oci:attr-get (statement-handle-of ,statement)
               oci:+htype-stmt+
               data-pointer
               size-pointer
               ,attribute
               (error-handle-of *transaction*)))
    (dereference-foreign-pointer data-pointer ,type size-pointer)))

(def function select-p (prepared-statement)
  (= oci:+stmt-select+
     (get-statement-attribute prepared-statement oci:+attr-stmt-type+ oci:ub-2)))

(def function insert-p (prepared-statement)
  (= oci:+stmt-insert+
     (get-statement-attribute prepared-statement oci:+attr-stmt-type+ oci:ub-2)))

(def function update-p (prepared-statement)
  (= oci:+stmt-update+
     (get-statement-attribute prepared-statement oci:+attr-stmt-type+ oci:ub-2)))

(defmacro with-foreign-oci-string ((string c-string c-size) &body body)
  `(with-falloc-string (,c-string ,c-size ,string nil
                                  (connection-encoding-of (database-of *transaction*))
                                  nil)
     ,@body))

(defmacro foreign-oci-string-alloc (string &optional null-terminate-p)
  `(with-falloc-string (bufp siz ,string :heap
                             (connection-encoding-of (database-of *transaction*))
                             ,null-terminate-p)
     (values bufp siz)))

(def function oci-string-to-lisp (pointer &optional size)
  (declare (optimize speed))
  #+nil
  (cffi:foreign-string-to-lisp pointer :count size
                               :encoding (connection-encoding-of (database-of *transaction*)))
  ;; the above doesn't work, because babel thinks the encoding is
  ;; invalid and returns question marks only.  Perhaps Babel doesn't
  ;; understand the endianness?  Need to investigate.
  (let* ((nchars (if size
		     ;; SIZE is an upper limit, but we cannot trust it:
		     (let ((limit (truncate size 2)))
		       (iter (for i from 0)
			     (repeat limit)
			     (when (eql 0 (cffi:mem-aref pointer :unsigned-short i))
			       (return i))
			     (finally (return limit))))
		     (iter (for i from 0)
			   (when (eql 0 (cffi:mem-aref pointer :unsigned-short i))
			     (return i)))))
	 (result (make-string nchars)))
    (iter (for i from 0 below nchars)
	  (setf (char result i) (code-char (cffi:mem-aref pointer :unsigned-short i))))
    result))

(def function oci-char-width ()
  (cffi::null-terminator-len (connection-encoding-of (database-of *transaction*)))) ;; FIXME using internal fn


(defmacro set-attribute (handle handle-type attribute value &optional (type 'oci:ub-4))
  `(with-falloc-object (data ,type 1 ,value)
     (oci-call (oci:attr-set ,handle
                             ,handle-type
                             data
                             ,(cffi:foreign-type-size type)
                             ,attribute
                             (error-handle-of *transaction*)))))

(defun set-string-attribute (handle handle-type attribute value)
  (with-foreign-oci-string (value c-string c-size)
    (oci-call (oci:attr-set handle
                            handle-type
                            c-string
                            c-size
                            attribute
                            (error-handle-of *transaction*)))))

(defmacro set-statement-attribute (statement attribute value &optional (type 'oci:ub-4))
  `(set-attribute (statement-handle-of ,statement)
                  oci:+htype-stmt+
                  ,attribute
                  ,value
                  ,type))

(defun set-session-string-attribute (attribute value)
  (set-string-attribute (session-handle-of *transaction*)
                        oci:+htype-session+
                        attribute
                        value))

(defmacro set-session-attribute (attribute value &optional (type 'oci:ub-4))
  `(set-attribute (session-handle-of *transaction*)
                  oci:+htype-session+
                  ,attribute
                  ,value
                  ,type))

(def function stmt-prepare (statement command)
  (with-foreign-oci-string (command c-command c-size)
    (oci-call (oci:stmt-prepare (statement-handle-of statement)
                                (error-handle-of *transaction*)
                                c-command
                                c-size
                                oci:+ntv-syntax+
                                *default-oci-flags*))))

(defun stmt-execute (statement mode nbatch)
  (oci-call (oci:stmt-execute (service-context-handle-of *transaction*)
                              (statement-handle-of statement)
                              (error-handle-of *transaction*)
                              (or nbatch (if (select-p statement) 0 1))
                              0
                              (cffi:null-pointer)
                              (cffi:null-pointer)
                              (if nbatch oci:+batch-mode+ mode))))

(defun %stmt-fetch-2 (stm nrows orientation offset)
  #+allegro
  (progn
    (assert (zerop offset))
    (oci:stmt-fetch (statement-handle-of stm)
                    (error-handle-of *transaction*)
                    nrows
                    orientation
                    *default-oci-flags*))
  #-allegro
  (oci:stmt-fetch-2 (statement-handle-of stm)
                    (error-handle-of *transaction*)
                    nrows
                    orientation
                    offset
                    *default-oci-flags*))

(defun %stmt-fetch-next (stm nrows)
  (%stmt-fetch-2 stm nrows oci:+fetch-next+ 0))

(defun stmt-fetch-2 (stm nrows orientation offset)
  (case (%stmt-fetch-2 stm nrows orientation offset)
    (#.oci:+success+ t)
    (#.oci:+success-with-info+ t) ;; warning, e.g. ORA-24347 sum over null
    (#.oci:+no-data+ nil)
    (t (handle-oci-error))))

(def function stmt-fetch-last (statement)
  (stmt-fetch-2 statement 1 oci:+fetch-last+ 0))

(def function stmt-fetch-next (statement number-of-rows)
  (stmt-fetch-2 statement number-of-rows oci:+fetch-next+ 0))

;;(defparameter *handle-stat* (list 26 0 27 0 9 0 2 0 8 0 3 0 4 0))

(defun handle-alloc (ptr type)
  ;;(incf (getf *handle-stat* type))
  (oci-call (oci:handle-alloc (environment-handle-of *transaction*)
                              ptr
                              type
                              0
                              (cffi:null-pointer))))

(defun handle-free (handle type)
  ;;(decf (getf *handle-stat* type))
  (oci-call (oci:handle-free handle type)))

(def function dump-c-byte-array (ptr size)
  (with-output-to-string (s)
                         (loop for i from 0 below size
                               do (format s "~2,'0X "
                                          (cffi:mem-ref ptr :uint8 i)))))

(def function descriptor-alloc (descriptor-ptr-ptr descriptor-type)
  (oci-call (oci:descriptor-alloc (environment-handle-of *transaction*)
                                  descriptor-ptr-ptr
                                  descriptor-type
                                  0
                                  (cffi:null-pointer))))

(def function allocate-oci-lob-locator (descriptor-ptr-ptr)
  (descriptor-alloc descriptor-ptr-ptr oci:+dtype-lob+))

(def function descriptor-free (descriptor-ptr descriptor-type)
  (oci-call (oci:descriptor-free descriptor-ptr
                                 descriptor-type)))

(def function free-oci-lob-locator (descriptor-ptr)
  (descriptor-free descriptor-ptr oci:+dtype-lob+))

(def function set-empty-lob (locator)
  (with-falloc-object (attribute oci:ub-4 1 0)
    (oci-call (oci:attr-set locator
                            oci:+dtype-lob+
                            attribute
                            0
                            oci:+attr-lobempty+
                            (error-handle-of *transaction*)))))

(def function make-lob-locator (&optional empty)
  (with-falloc-object (descriptor-ptr-ptr :pointer)
    (allocate-oci-lob-locator descriptor-ptr-ptr)
    (let ((locator (cffi:mem-aref descriptor-ptr-ptr :pointer)))
      (when empty
        (set-empty-lob locator))
      (values locator #.(cffi:foreign-type-size :pointer)))))

(defun make-lob-locator-indirect (&optional empty)
  (values
   (heap-falloc :pointer 1 (make-lob-locator empty))
   #.(cffi:foreign-type-size :pointer)))

(def function clob-type-p (sql-type)
  (typep sql-type 'sql-character-large-object-type))

(def function blob-type-p (sql-type)
  (typep sql-type 'sql-binary-large-object-type))

(def function lob-type-p (sql-type)
  (or (clob-type-p sql-type)
      (blob-type-p sql-type)))

(def function lob-write (svchp errhp locator bufp siz &optional amt csid)
  (with-falloc-object (amtp oci:sb-4 1 (or amt siz))
    (oci-call (oci:lob-write svchp errhp locator amtp 1 bufp siz oci:+one-piece+
                             (cffi:null-pointer) (cffi:null-pointer) (or csid 0) oci:+sqlcs-implicit+))
    (assert (= (or amt siz) (cffi:mem-ref amtp 'oci:sb-4)))))

(defun call-with-lob-buffering (locator fn)
  (let ((svchp (service-context-handle-of *transaction*))
        (errhp (error-handle-of *transaction*)))
    (oci-call (oci:lob-enable-buffering svchp errhp locator))
    (unwind-protect (funcall fn locator)
      (oci-call (oci:lob-disable-buffering svchp errhp locator)))))

(defmacro with-lob-buffering ((locator) &body body)
  `(call-with-lob-buffering ,locator (lambda (,locator) ,@body)))

(def function lob-flush-buffer (svchp errhp locator)
  (oci-call (oci:lob-flush-buffer svchp errhp locator oci:+lob-buffer-nofree+)))

(defun upload-clob (locator value)
  (assert (plusp (length value)))
  (let ((svchp (service-context-handle-of *transaction*))
        (errhp (error-handle-of *transaction*)))
    (with-foreign-oci-string (value bufp siz)
      (lob-write svchp errhp locator bufp siz (length value) oci:+utf-16-id+))))

(defun upload-blob (locator value)
  (assert (plusp (length value)))
  (let ((svchp (service-context-handle-of *transaction*))
        (errhp (error-handle-of *transaction*))
        (siz (length value)))
    (with-falloc-object (bufp oci:ub-1 siz)
      (loop
         for byte across value
         for i from 0
         below siz ;; stop at the vector's fill-pointer
         do (setf (cffi:mem-aref bufp 'oci:ub-1 i) byte))
      (lob-write svchp errhp locator bufp siz))))

(defun upload-lob (locator value)
  (etypecase value
    (string (upload-clob locator value))
    (vector (upload-blob locator value))))

(defun lob-chunk-size (svchp errhp locp)
  (with-falloc-object (chunksizep oci:ub-4)
    (oci-call (oci:lob-get-chunk-size svchp errhp locp chunksizep))
    (cffi:mem-ref chunksizep 'oci:ub-4)))
