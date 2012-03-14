(in-package :hu.dwim.rdbms.oracle)

;;; falloc is a custom memory manager operating on a block of foreign
;;; memory.  It is meant to be allocated per transaction so that
;;; working with oci functions doesn't cause individual cffi
;;; allocations.  Heap allocations are "freed" implicitly per
;;; statement execution.

;; TODO THL ideally, remove heap allocations.  Anything that uses the
;; heap allocations can cause problems.  At the moment, only binders
;; use it, but so far we don't use bulk inserts or updates so the heap
;; allocated data are small.  Binders could/should be rewriten to use
;; stack allocation and then meaybe the heap option dropped
;; completely.

(defstruct (falloc (:constructor %make-falloc))
  base
  (ht 0 :type fixnum)
  (sb 0 :type fixnum)
  (n 0 :type fixnum)
  (maxh 0 :type fixnum)
  (maxs 0 :type fixnum)
  (gap 0 :type fixnum))

(cffi:defcfun "malloc" :pointer
  (nbytes oci:size-t))

(cffi:defcfun "free" :void
  (ptr :pointer))

(defun make-falloc (nbytes)
  (%make-falloc :base (malloc nbytes) #+nil(cffi:foreign-alloc :uint8 :count nbytes)
                :ht 0
                :sb nbytes
                :n nbytes
                :maxh 0
                :maxs 0
                :gap nbytes))

(defun free-falloc (x)
  (free #+nil cffi-sys:foreign-free (falloc-base x)))

;; http://lambda-the-ultimate.org/node/1997#comment-24650
(defmacro with-struct ((conc-name &rest names) obj &body body)
  "Like with-slots but works only for structs."
  (flet ((reader (slot) (intern (concatenate 'string
					     (symbol-name conc-name)
					     (symbol-name slot))
				(symbol-package conc-name))))
    (let ((tmp (gensym "OO-")))
      ` (let ((,tmp ,obj))
          (symbol-macrolet
              ,(loop for name in names collect
                    (typecase name
                      (symbol `(,name (,(reader name) ,tmp)))
                      (cons `(,(first name) (,(reader (second name)) ,tmp)))
                      (t (error "Malformed syntax in WITH-STRUCT: ~A" name))))
            ,@body)))))

(defun reset-falloc (x)
  (with-struct (falloc- ht sb n) x
    (setq ht 0
          sb n))
  x)

#+allegro
(cffi:defcfun "memset" :pointer
  (bufp :pointer)
  (val :int)
  (len oci:size-t))

#+allegro
(defun %initialize8 (base offset cnt val)
  (declare (optimize (speed 3) (safety 0))
           (fixnum offset cnt val))
  (dotimes (i cnt)
    (declare (fixnum i))
    (macrolet ((mref (n)
                 `(system:memref-int base offset ,n :unsigned-byte)))
      (setf (mref i) val))))

#+allegro
(defun %initialize8vec (base offset cnt val)
  (declare (optimize (speed 3) (safety 0))
           (fixnum offset cnt))
  (dotimes (i cnt)
    (declare (fixnum i))
    (let ((x (svref val i)))
      (declare (fixnum x))
      (macrolet ((mref (n)
                   `(system:memref-int base offset ,n :unsigned-byte)))
        (setf (mref i) x)))))

#+allegro
(defun %initialize16 (base offset cnt val)
  (declare (optimize (speed 3) (safety 0))
           (fixnum offset cnt val))
  (let ((v0 (logand #xff val))
        (v1 (logand #xff (setq val (ash val -8))))
        (j -1))
    (declare (fixnum v0 v1 j))
    (dotimes (i cnt)
      (declare (fixnum i))
      (macrolet ((mref (n)
                   `(system:memref-int base offset ,n :unsigned-byte)))
        (setf (mref (incf j)) v0
              (mref (incf j)) v1)))))

#+allegro
(defun %initialize32 (base offset cnt val)
  (declare (optimize (speed 3) (safety 0))
           (fixnum offset cnt))
  (let ((v0 (logand #xff val))
        (v1 (logand #xff (setq val (ash val -8))))
        (v2 (logand #xff (setq val (ash val -8))))
        (v3 (logand #xff (setq val (ash val -8))))
        (j -1))
    (declare (fixnum v0 v1 v2 v3 j))
    (dotimes (i cnt)
      (declare (fixnum i))
      (macrolet ((mref (n)
                   `(system:memref-int base offset ,n :unsigned-byte)))
        (setf (mref (incf j)) v0
              (mref (incf j)) v1
              (mref (incf j)) v2
              (mref (incf j)) v3)))))

#+allegro
(defun %initialize64 (base offset cnt val)
  (declare (optimize (speed 3) (safety 0))
           (fixnum offset cnt))
  (let ((v0 (logand #xff val))
        (v1 (logand #xff (setq val (ash val -8))))
        (v2 (logand #xff (setq val (ash val -8))))
        (v3 (logand #xff (setq val (ash val -8))))
        (v4 (logand #xff (setq val (ash val -8))))
        (v5 (logand #xff (setq val (ash val -8))))
        (v6 (logand #xff (setq val (ash val -8))))
        (v7 (logand #xff (setq val (ash val -8))))
        (j -1))
    (declare (fixnum v0 v1 v2 v3 v4 v5 v6 v7 j))
    (dotimes (i cnt)
      (declare (fixnum i))
      (macrolet ((mref (n)
                   `(system:memref-int base offset ,n :unsigned-byte)))
        (setf (mref (incf j)) v0
              (mref (incf j)) v1
              (mref (incf j)) v2
              (mref (incf j)) v3
              (mref (incf j)) v4
              (mref (incf j)) v5
              (mref (incf j)) v6
              (mref (incf j)) v7)))))

(defun initialize-falloc-object (base offset cnt val type)
  #+allegro
  (declare (optimize (speed 3) (safety 0))
           (fixnum offset cnt))
  (let ((bufp (cffi-sys:inc-pointer base offset)))
    (when val
      #+allegro
      (macrolet ((mref (i typ)
                   `(system:memref-int base offset ,i ',typ :coerce))
                 (one (i typ v)
                   `(setf (mref ,i ,typ) ,v))
                 (doop (w typ v)
                   `(loop
                       for i from 0 below cnt by ,w
                       do (one i ,typ ,v)))
                 (expand (w typ)
                   `(cond
                      ((vectorp val) (doop ,w ,typ (aref val i)))
                      (t (doop ,w ,typ val)))))
        (if (atom val)
            (ecase type
              (:double (expand 8 :double-float))
              (:float (expand 4 :single-float))
              ((oci:sb-1 :uint8 oci:ub-1)
               (assert (typep val 'fixnum))
               (memset bufp val cnt)
               #+nil
               (%initialize8 base offset cnt val))
              ((:short oci:sb-2 :unsigned-short oci:ub-2)
               (assert (typep val 'fixnum))
               (%initialize16 base offset cnt val))
              ((:pointer oci:sb-4 oci:ub-4) (%initialize32 base offset cnt val))
              (oci:oraub-8 (%initialize64 base offset cnt val)))
            (ecase type
              (oci:ub-1 (%initialize8vec base offset cnt val)))))
      #-allegro
      (macrolet ((one (i typ v)
                   `(setf (cffi:mem-aref bufp ',typ ,i) ,v))
                 (expand (typ)
                   `(cond
                      ((vectorp val) (dotimes (i cnt) (one i ,typ (aref val i))))
                      (t (dotimes (i cnt) (one i ,typ val))))))
        (ecase type
          (:pointer (expand :pointer))
          (:double (expand :double))
          (:float (expand :float))
          (:uint8 (expand :uint8))
          (:unsigned-short (expand :unsigned-short))
          (:short (expand :short))
          (oci:sb-1 (expand oci:sb-1))
          (oci:sb-2 (expand oci:sb-2))
          (oci:sb-4 (expand oci:sb-4))
          (oci:ub-1 (expand oci:ub-1))
          (oci:ub-2 (expand oci:ub-2))
          (oci:ub-4 (expand oci:ub-4))
          (oci:oraub-8 (expand oci:oraub-8)))))
    bufp))

(defvar *falloc*)

(defun call-with-falloc-object (type nbytes1 count value heap fn)
  (assert (and (positive-fixnum-p nbytes1) (positive-fixnum-p count)))
  (with-struct (falloc- base ht sb n maxh maxs gap) *falloc*
    (macrolet ((check ()
                 `(progn
                    (assert (<= 0 ht sb n))
                    (setq maxh (max maxh ht)
                          maxs (max maxs (- n sb))
                          gap  (min gap (- sb ht))))))
      (check)
      (let ((nbytes (* nbytes1 count)))
        (let ((m (mod nbytes 4))) ;; 4B alignment
          (unless (zerop m)
            (setq nbytes (+ nbytes (- 4 m)))))
        ;;(assert (zerop (mod nbytes 4)))
        (if heap
            (let ((x ht)
                  (y (setq ht (+ ht nbytes))))
              ;;(assert (zerop (mod ht 4)))
              (check)
              (unwind-protect
                   (funcall fn
                            (initialize-falloc-object base x count value type))
                (check)
                (assert (<= y ht))))
            (let ((x sb)
                  (y (setq sb (- sb nbytes))))
              ;;(assert (zerop (mod sb 4)))
              (check)
              (unwind-protect
                   (funcall fn
                            (initialize-falloc-object base y count value type))
                (check)
                (setq sb x)
                (check))))))))

(defmacro with-falloc-object ((var type &optional (count 1) value heap) &body body)
  `(call-with-falloc-object ',type ,(cffi:foreign-type-size type) ,count ,value
                            ,heap (lambda (,var) ,@body)))

(defun call-with-falloc-string (string heap encoding null-terminated-p start end fn)
  ;; based on cffi::%foreign-string-alloc
  (check-type string string)
  (cffi::with-checked-simple-vector ((string (coerce string 'babel:unicode-string))
                                     (start start)
                                     (end end))
    (declare (type simple-string string))
    (let* ((mapping (cffi::lookup-mapping cffi::*foreign-string-mappings* encoding))
           (count (funcall (cffi::octet-counter mapping) string start end 0))
           (tlen (cffi::null-terminator-len encoding))
           (length (if null-terminated-p (+ count tlen) count)))
      (with-falloc-object (ptr oci:ub-1 length nil heap)
        (funcall (cffi::encoder mapping) string start end ptr 0)
        (when null-terminated-p
          (dotimes (i tlen)
            (setf (cffi:mem-ref ptr 'oci:ub-1 (+ count i)) 0)))
        (funcall fn ptr length)))))

(defmacro with-falloc-string ((bufp-var len-var value &optional heap
                                        (encoding 'cffi:*default-foreign-encoding*)
                                        (null-terminated-p t)
                                        (start 0)
                                        end)
                              &body body)
  `(call-with-falloc-string ,value ,heap ,encoding ,null-terminated-p ,start ,end
                            (lambda (,bufp-var ,len-var) ,@body)))

(defmacro with-falloc-objects (bindings &body body)
  (if bindings
      `(with-falloc-object ,(car bindings)
         (with-falloc-objects ,(cdr bindings)
           ,@body))
      `(progn ,@body)))

(defun heap-falloc (type &optional (count 1) value)
  (macrolet ((expand (typ)
               `(with-falloc-object (x ,typ count value :heap)
                  x)))
    (ecase type
      (:pointer (expand :pointer))
      (:float (expand :float))
      (:double (expand :double))
      (oci:sb-1 (expand oci:sb-1))
      (oci:sb-2 (expand oci:sb-2))
      (oci:sb-4 (expand oci:sb-4))
      (oci:ub-1 (expand oci:ub-1))
      (oci:ub-2 (expand oci:ub-2))
      (oci:ub-4 (expand oci:ub-4))
      (oci:date (expand oci:date)))))

(defparameter *falloc-size* (* 10 (expt 1024 2)))

(defvar *falloc-pool* nil)
(defvar *falloc-pool-lock* (bordeaux-threads:make-lock))

(defun call-with-falloc (fn)
  (assert (not (boundp '*falloc*)))
  (let ((*falloc* (reset-falloc
                   (or (bordeaux-threads:with-lock-held (*falloc-pool-lock*)
                         (pop *falloc-pool*))
                       (make-falloc *falloc-size*)))))
    (unwind-protect (funcall fn)
      (bordeaux-threads:with-lock-held (*falloc-pool-lock*)
        (push *falloc* *falloc-pool*)))))

(defmacro with-falloc (() &body body)
  `(call-with-falloc (lambda () ,@body)))

(defun free-falloc-pool ()
  (bordeaux-threads:with-lock-held (*falloc-pool-lock*)
    (mapc 'free-falloc *falloc-pool*)
    (setq *falloc-pool* nil)))

#+nil
(with-falloc ()
  (with-falloc-object (x oci:oraub-8 3 nil t)
    (with-falloc-object (y oci:ub-4 5)
      (list x y (heap-falloc :pointer 2) (falloc-sb *falloc*) (falloc-s *falloc*) *falloc*))))
