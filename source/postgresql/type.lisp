;;; -*- mode: Lisp; Syntax: Common-Lisp; -*-
;;;
;;; Copyright (c) 2006 by the authors.
;;;
;;; See LICENCE for details.

(in-package :hu.dwim.rdbms.postgresql)

(def method format-sql-syntax-node ((type sql-float-type) (database postgresql))
  (let ((bit-size (bit-size-of type)))
    (cond ((null bit-size)
           (format-string "FLOAT8"))
          ((<= bit-size 32)
           (format-string "FLOAT4"))
          ((<= bit-size 64)
           (format-string "FLOAT8"))
          (t (error "Unknown bit size for ~A" type)))))

(def method format-sql-syntax-node ((type sql-character-large-object-type) (database postgresql))
  (format-string "TEXT"))

(def method format-sql-syntax-node ((type sql-binary-large-object-type) (database postgresql))
  (format-string "BYTEA"))

;; http://stackoverflow.com/questions/3350148/where-are-numeric-precision-and-scale-for-a-field-found-in-the-pg-catalog-tables
(defun %numeric-type (typname attlen atttypmod attnotnull)
  (or (if (= -1 atttypmod)
          (sql-numeric-type)
          (let ((p (logand #xffff (ash (- atttypmod 4) -16)))
                (s (logand #xffff (- atttypmod 4))))
            (when (plusp s)
              ;; sql-numeric-type is too vague, the only case where
              ;; scale is used is the (exact) decimal type, e.g. for
              ;; representing money
              (sql-decimal-type :precision p :scale s))))
      (error "unexpected sql type ~s ~s ~s ~s"
             typname attlen atttypmod attnotnull)))

(def function sql-type-for-internal-type (description)
  (let ((type-name (first-elt description)))
    (flet ((native-size (x)
             (unless (= -1 x)
               (- x 4))))
      (macrolet ((sql-type-case (&body entries)
                   `(progn
                     ,@(loop for (name . body) :in entries
                             collect `(when (equalp type-name ,name)
                                       (return-from sql-type-for-internal-type (progn ,@body)))))))
        (sql-type-case ("int2" (make-instance 'sql-integer-type :bit-size 16))
                       ("int4" (make-instance 'sql-integer-type :bit-size 32))
                       ("int8" (make-instance 'sql-integer-type :bit-size 64))
                       ("float4" (make-instance 'sql-float-type :bit-size 32))
                       ("float8" (make-instance 'sql-float-type :bit-size 64))
                       ("numeric" (apply '%numeric-type (coerce description 'list)))
                       ("bool" (make-instance 'sql-boolean-type))
                       ("char" (make-instance 'sql-character-type :size (native-size (elt description 2))))
                       ("bpchar" (make-instance 'sql-character-type :size (native-size (elt description 2))))
                       ("varchar" (make-instance 'sql-character-varying-type
                                                 :size (native-size (elt description 2))))
                       ("text" (make-instance 'sql-character-large-object-type))
                       ("tsvector" (make-instance 'sql-tsvector-type))
                       ("bytea" (make-instance 'sql-binary-large-object-type))
                       ("date" (make-instance 'sql-date-type))
                       ("time" (make-instance 'sql-time-type))
                       ("timestamp" (make-instance 'sql-timestamp-type))
                       ("timestamptz" (make-instance 'sql-timestamp-with-timezone-type)))
        (error "Unknown internal type")))))
