#ifndef TIME_UTILS_H
#define TIME_UTILS_H

/*
 * Monotonic clock helpers for rate-matched dequeue pacing.
 * All timestamps use CLOCK_MONOTONIC.
 */

#include <time.h>

typedef enum {
    TU_OK = 0,
    TU_ERR_INVALID = -1,
    TU_ERR_SYSTEM = -2
} TimeUtilsStatus;

/* Read CLOCK_MONOTONIC into *ts. */
TimeUtilsStatus time_utils_now_mono(struct timespec *ts);

/*
 * out = a - b
 * Returns TU_ERR_INVALID if the result would be negative.
 */
TimeUtilsStatus time_utils_ts_sub(const struct timespec *a,
                                  const struct timespec *b,
                                  struct timespec *out);

/* out = a + b (nanosecond overflow normalized). */
TimeUtilsStatus time_utils_ts_add(const struct timespec *a,
                                  const struct timespec *b,
                                  struct timespec *out);

/*
 * Sleep for duration using clock_nanosleep(CLOCK_MONOTONIC, 0, ...).
 * Retries on EINTR.
 */
TimeUtilsStatus time_utils_sleep_for(const struct timespec *duration);

/*
 * Sleep until absolute monotonic deadline.
 * Returns immediately if deadline is already in the past.
 */
TimeUtilsStatus time_utils_sleep_until(const struct timespec *deadline);

/*
 * Return 1 if a is strictly before b, else 0.
 * a and b must be non-NULL.
 */
int time_utils_ts_before(const struct timespec *a, const struct timespec *b);

#endif /* TIME_UTILS_H */
