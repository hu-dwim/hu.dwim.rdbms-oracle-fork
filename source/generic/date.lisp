;;; -*- mode: Lisp; Syntax: Common-Lisp; -*-
;;;
;;; Copyright (c) 2006 by the authors.
;;;
;;; See LICENCE for details.

(in-package :hu.dwim.rdbms)

;;;;;;
;;; a real DATE type that never gets munged accidentially due to timezoning
;;; issues -- by virtue of not having hour/minute slots at all.

(def (class* e) cdate ()
  (y m d))

(def (function e) make-cdate (y m d)
  (check-type y (integer 1 9999))
  (check-type m (integer 1 12))
  (check-type d (integer 1 31))
  (make-instance 'cdate :y y :m m :d d))

(defmethod print-object ((object cdate) stream)
  (print-unreadable-object (object stream :type t :identity nil)
    (format-cdate-iso-string object stream)))

(def (function e) cdate= (a b)
  (and (eql (y-of a) (y-of b))
       (eql (m-of a) (m-of b))
       (eql (d-of a) (d-of b))))

(def (function e) decode-cdate (cdate)
  (values (y-of cdate) (m-of cdate) (d-of cdate)))

(def (function e) cdate-to-iso-string (cdate)
  (format-cdate-iso-string cdate nil))

(def (function e) format-cdate-iso-string (cdate stream)
  (multiple-value-bind (y m d) (decode-cdate cdate)
    (format stream "~4,'0D-~2,'0D-~2,'0D" y m d)))

(def (function e) cdate-from-iso-string (string)
  (let ((y (parse-integer (subseq string 0 4)))
        (m (parse-integer (subseq string 5 7)))
        (d (parse-integer (subseq string 8 10))))
    (make-cdate y m d)))

;; when using these functions, take care to think about timezone issues;
;; by default we're using CDATE instead of LOCAL-TIME for a reason.
(def (function e) cdate-local-time (cdate)
  (multiple-value-bind (y m d) (decode-cdate cdate)
    (local-time:encode-timestamp
     0 0 0 0 d m y
     :timezone local-time:+utc-zone+)))
;;
(def (function e) local-time-to-cdate (value)
  (unless (and (zerop (local-time:sec-of value))
	       (zerop (local-time:nsec-of value)))
    (cerror "continue"
	    "Converting a local-time date value as ~S with non-zero time values; time values will be silently dropped! The timestamp in question is: ~A"
	    'sql-date-type value))
  (multiple-value-bind (nsec sec minute hour day month year)
      (local-time:decode-timestamp value :timezone local-time:+utc-zone+)
    (assert (zerop nsec))
    (assert (zerop sec))
    (assert (zerop minute))
    (assert (zerop hour))
    (make-cdate year month day)))

(def (function e) cdate-from-unix (unix-time)
  (local-time-to-cdate (local-time:unix-to-timestamp unix-time)))
