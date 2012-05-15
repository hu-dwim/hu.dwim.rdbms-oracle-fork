;;; -*- mode: Lisp; Syntax: Common-Lisp; -*-
;;;
;;; Copyright (c) 2006 by the authors.
;;;
;;; See LICENCE for details.

(in-package :hu.dwim.rdbms.oracle)

(defstruct typemap
  external-type
  lisp-to-oci)

;; TODO add a naming convention, maybe *foo*?
(def definer typemap (name &rest args)
  `(def special-variable ,name (make-typemap ,@args)))

(def typemap boolean/char
    :external-type oci:+sqlt-afc+
    :lisp-to-oci 'boolean-to-char)

(def typemap integer/int8
    :external-type oci:+sqlt-int+
    :lisp-to-oci 'integer-to-int8)

(def typemap integer/int16
    :external-type oci:+sqlt-int+
    :lisp-to-oci 'integer-to-int16)

(def typemap integer/int32
    :external-type oci:+sqlt-int+
    :lisp-to-oci 'integer-to-int32)

(def typemap integer/varnum
    :external-type oci:+sqlt-vnu+
    :lisp-to-oci 'integer-to-varnum)     ;; TODO: pass precision/scale

(def typemap float/bfloat
    :external-type oci:+sqlt-bfloat+
    :lisp-to-oci 'float-to-bfloat)

(def typemap double/bdouble
    :external-type oci:+sqlt-bdouble+
    :lisp-to-oci 'double-to-bdouble)

(def typemap rational/varnum
    :external-type oci:+sqlt-vnu+
    :lisp-to-oci 'rational-to-varnum)      ;; TODO: pass precision/scale

(def typemap string/string
    :external-type oci:+sqlt-str+
    :lisp-to-oci 'string-to-string)

(def typemap string/clob
    :external-type oci:+sqlt-clob+
    :lisp-to-oci 'string-to-clob)

(def typemap cdate/date
    :external-type oci:+sqlt-dat+
    :lisp-to-oci 'cdate-to-date)

(def typemap cdate/oci-date
    :external-type oci:+sqlt-odt+
    :lisp-to-oci 'cdate-to-oci-date)

(def typemap local-time/time
    :external-type oci:+sqlt-timestamp+
    :lisp-to-oci 'local-time-to-time)

(def typemap local-time/timestamp
    :external-type oci:+sqlt-timestamp+
    :lisp-to-oci 'local-time-to-timestamp)

(def typemap local-time/timestamp-tz
    :external-type oci:+sqlt-timestamp-tz+
    :lisp-to-oci 'local-time-to-timestamp-tz)

(def typemap byte-array/blob
    :external-type oci:+sqlt-blob+
    :lisp-to-oci 'byte-array-to-blob)

(defun make-typemap/rational (precision scale)
  (make-typemap
   :external-type oci:+sqlt-vnu+
   :lisp-to-oci (lambda (x)
                  (assert (typep (* x (expt 10 scale)) 'integer))
                  (rational-to-varnum x :precision precision :scale scale))))

(defun typemap-for-sql-type (type)
  (etypecase type
    (sql-boolean-type
      ;; booleans are stored as CHAR(1) internally
      boolean/char)
    (sql-integer-type
      ;; integers are stored as NUMBER(x) internally
      ;; where x=3  for 8-bit integers
      ;;       x=5  for 16-bit integers
      ;;       x=10 for 32-bit integers
      ;;       x=38 for bigger integers
      ;; their external type is byte/short/int or byte[22] (varnum format)
      ;; XXX OCI does not have external type for int64?
      (with-slots (bit-size) type
        (cond
          ((not bit-size) integer/varnum)
          ((<= bit-size 8) integer/int8)
          ((<= bit-size 16) integer/int16)
          ((<= bit-size 32) integer/int32)
          (t integer/varnum))))
    (sql-float-type
      ;; floats are stored as BINARY_FLOAT or BINARY_DOUBLE internally
      ;; their external type is float/double
      (with-slots (bit-size) type
        (assert (and bit-size (<= 32 bit-size 64)))
        (cond
          ((<= bit-size 32) float/bfloat)
          ((<= bit-size 64) double/bdouble))))
    (sql-numeric-type
      ;; numeric values are stored as NUMBER internally
      ;; their external type is byte[22] (varnum)
      ;; NOTE: when rationals stored in a numeric column, their precision may be lost
      ;;       e.g. 1/3 -> 3333.../10000...
      (error "use more specific type with oracle backend") ;; TODO THL handle this better?
      rational/varnum)
    (sql-decimal-type
      (make-typemap/rational (precision-of type) (scale-of type)))
    (sql-character-type
      ;; string values stored as CHAR(x) internally
      ;; their external format is zero terminated string
      string/string)
    (sql-character-varying-type
      ;; string values stored as VARCHAR2(x) internally
      ;; their external format is zero terminated string
      string/string)
    (sql-character-large-object-type string/clob)
    (sql-date-type cdate/date)
    (sql-time-type local-time/time)
    (sql-timestamp-type local-time/timestamp)
    (sql-timestamp-with-timezone-type local-time/timestamp-tz)
    (sql-binary-large-object-type byte-array/blob)))

(def function sql-type-for-internal-type (data-type char-length precision scale)
  (macrolet ((estringcase (keyform &body clauses)
               `(cond
                 ,@(mapcar (lambda (clause)
                             `((string= ,(first clause) ,keyform) ,@(rest clause)))
                           clauses)
                 (t (error "Falling through estringcase: ~S" ,keyform)))))
    (estringcase data-type
     ("NUMBER" (if (eql 0 scale)
                   (if precision
                       (case precision
                         (5 (sql-integer-type :bit-size 16)) ; KLUDGE
                         (10 (sql-integer-type :bit-size 32)) ; KLUDGE
                         (19 (sql-integer-type :bit-size 64)) ; KLUDGE
                         (t (sql-integer-type)))  ; FIXME bit-size lost
                       (sql-integer-type))
                   (sql-numeric-type)))      ; FIXME scale, precision?
     ("BINARY_FLOAT" (sql-float-type :bit-size 32))
     ("BINARY_DOUBLE" (sql-float-type :bit-size 64))
     ("CHAR" (if (= char-length 1)
                 (sql-boolean-type)     ; KLUDGE: boolean as CHAR(1)
                 (sql-character-type :size char-length)))
     ("VARCHAR2" (sql-character-varying-type :size char-length))
     ("CLOB" (sql-character-large-object-type)) ; FIXME size not mapped
     ("BLOB" (sql-binary-large-object-type)) ; FIXME size not mapped
     ("RAW" (sql-binary-large-object-type :size char-length)) ;; for db reflection only
     ("DATE" (sql-date-type))
     ("TIMESTAMP(6)" (sql-timestamp-type))
     ("TIMESTAMP(6) WITH TIME ZONE" (sql-timestamp-with-timezone-type)))))


(def function external-type-for-sql-type (type)
  (typemap-external-type (typemap-for-sql-type type)))

(def function typemap-for-internal-type (internal-type size &key precision scale)
  (declare (fixnum internal-type))
  (ecase internal-type
    (#.oci:+sqlt-chr+ string/string)    ; varchar
    (#.oci:+sqlt-afc+                   ; char, boolean
     (if (= size 1)
         boolean/char    ; KLUDGE char(1) assumed to be a boolean
         string/string))
    (#.oci:+sqlt-num+
     ;;#.oci:+sqlt-vnu+
     (if (and (<= scale 0) (<= (- precision scale) 9))
         integer/varnum
         (make-typemap/rational precision scale)))
    (#.oci:+sqlt-dat+ cdate/date)
    (#.oci:+sqlt-ibfloat+ float/bfloat)
    (#.oci:+sqlt-ibdouble+ double/bdouble)
    (#.oci:+sqlt-timestamp+ local-time/timestamp)    ; CHECK: was 180
    (#.oci:+sqlt-timestamp-tz+ local-time/timestamp-tz) ; CHECK: was 181
    (#.oci:+sqlt-clob+ string/clob)
    (#.oci:+sqlt-blob+ byte-array/blob)))

(def function data-size-for (external-type column-size)
  (declare (fixnum external-type))
  (ecase external-type
    (#.oci:+sqlt-afc+ (* (oci-char-width) column-size))
    (#.oci:+sqlt-int+ 4)
    (#.oci:+sqlt-vnu+ 22)
    (#.oci:+sqlt-bfloat+ 4)
    (#.oci:+sqlt-bdouble+ 8)
    (#.oci:+sqlt-str+ (* (oci-char-width) (1+ column-size)))
    #+nil(#.oci:+sqlt-lvc+ (min (+ column-size 4) 8000)) ; FIXME
    (#.oci:+sqlt-dat+ 7)
    (#.oci:+sqlt-odt+ #.(cffi:foreign-type-size 'oci:date))
    (#.oci:+sqlt-timestamp+ #.(cffi:foreign-type-size :pointer))
    (#.oci:+sqlt-timestamp-tz+ #.(cffi:foreign-type-size :pointer))
    (#.oci:+sqlt-clob+ #.(cffi:foreign-type-size :pointer))
    (#.oci:+sqlt-blob+ #.(cffi:foreign-type-size :pointer))
    #+nil(#.oci:+sqlt-lvb+ (min (+ column-size 4) 8000)))) ; FIXME
