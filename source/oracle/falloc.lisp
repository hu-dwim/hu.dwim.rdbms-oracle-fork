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

(defun make-falloc (nbytes)
  (%make-falloc :base (cffi:foreign-alloc :uint8 :count nbytes)
                :ht 0
                :sb nbytes
                :n nbytes
                :maxh 0
                :maxs 0
                :gap nbytes))

(defun free-falloc (x)
  (cffi-sys:foreign-free (falloc-base x)))

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

(defvar *falloc*)

(defun call-with-falloc-object (nbytes1 count value heap fn value-fn)
  (assert (and (plusp nbytes1) (plusp count)))
  (with-struct (falloc- base ht sb n maxh maxs gap) *falloc*
    (macrolet ((p (x)
                 `(cffi-sys:inc-pointer base ,x))
               (check ()
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
              (unwind-protect (funcall fn (funcall value-fn (p x) count value))
                (check)
                (assert (<= y ht))))
            (let ((x sb)
                  (y (setq sb (- sb nbytes))))
              ;;(assert (zerop (mod sb 4)))
              (check)
              (unwind-protect (funcall fn (funcall value-fn (p y) count value))
                (check)
                (setq sb x)
                (check))))))))

(defmacro with-falloc-object ((var type &optional (count 1) value heap) &body body)
  `(call-with-falloc-object
    ,(cffi:foreign-type-size type) ,count ,value ,heap
    (lambda (,var) ,@body)
    (lambda (bufp cnt val)
      (when val
        #+allegro
        ,(cond
          ((eq 'oci:oraub-8 type)
           `(cond
              ((vectorp val)
               (dotimes (i cnt)
                 (let ((v (aref val i)))
                   (setf (cffi:mem-aref bufp 'oci:ub-4 (* 2 i))
                         (logand #xffffffff v)
                         (cffi:mem-aref bufp 'oci:ub-4 (1+ (* 2 i)))
                         (ash v -32)))))
              (t
               (dotimes (i cnt)
                 (setf (cffi:mem-aref bufp 'oci:ub-4 (* 2 i))
                       (logand #xffffffff val)
                       (cffi:mem-aref bufp 'oci:ub-4 (1+ (* 2 i)))
                       (ash val -32))))))
          (t
           `(cond
              ((vectorp val)
               (dotimes (i cnt)
                 (setf (cffi:mem-aref bufp ',type i) (aref val i))))
              (t
               (dotimes (i cnt)
                 (setf (cffi:mem-aref bufp ',type i) val))))))
        #-allegro
        (cond
          ((vectorp val)
           (dotimes (i cnt)
             (setf (cffi:mem-aref bufp ',type i) (aref val i))))
          (t
           (dotimes (i cnt)
             (setf (cffi:mem-aref bufp ',type i) val)))))
      bufp)))

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
      (with-falloc-object (ptr :char length nil heap)
        (funcall (cffi::encoder mapping) string start end ptr 0)
        (when null-terminated-p
          (dotimes (i tlen)
            (setf (cffi:mem-ref ptr :char (+ count i)) 0)))
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

(defparameter *falloc-size* (* 3 (expt 1024 2)))

(defvar *falloc-pool* nil)
(defvar *falloc-pool-lock* (bordeaux-threads:make-lock))

(defun call-with-falloc (fn)
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
