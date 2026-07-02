#include "time_utils.h"

#include <errno.h>

#define NS_PER_SEC 1000000000L

static TimeUtilsStatus normalize_ts(struct timespec *ts)
{
    if (ts->tv_nsec >= NS_PER_SEC) {
        ts->tv_sec += ts->tv_nsec / NS_PER_SEC;
        ts->tv_nsec %= NS_PER_SEC;
    } else if (ts->tv_nsec < 0) {
        return TU_ERR_INVALID;
    }

    return TU_OK;
}

TimeUtilsStatus time_utils_now_mono(struct timespec *ts)
{
    if (ts == NULL) {
        return TU_ERR_INVALID;
    }

    if (clock_gettime(CLOCK_MONOTONIC, ts) != 0) {
        return TU_ERR_SYSTEM;
    }

    return TU_OK;
}

TimeUtilsStatus time_utils_ts_sub(const struct timespec *a,
                                  const struct timespec *b,
                                  struct timespec *out)
{
    struct timespec result;

    if (a == NULL || b == NULL || out == NULL) {
        return TU_ERR_INVALID;
    }

    result.tv_sec = a->tv_sec - b->tv_sec;
    result.tv_nsec = a->tv_nsec - b->tv_nsec;

    if (result.tv_nsec < 0) {
        result.tv_sec--;
        result.tv_nsec += NS_PER_SEC;
    }

    if (result.tv_sec < 0) {
        return TU_ERR_INVALID;
    }

    *out = result;
    return TU_OK;
}

TimeUtilsStatus time_utils_ts_add(const struct timespec *a,
                                  const struct timespec *b,
                                  struct timespec *out)
{
    struct timespec result;

    if (a == NULL || b == NULL || out == NULL) {
        return TU_ERR_INVALID;
    }

    result.tv_sec = a->tv_sec + b->tv_sec;
    result.tv_nsec = a->tv_nsec + b->tv_nsec;

    if (normalize_ts(&result) != TU_OK) {
        return TU_ERR_INVALID;
    }

    if (result.tv_nsec >= NS_PER_SEC) {
        result.tv_sec += result.tv_nsec / NS_PER_SEC;
        result.tv_nsec %= NS_PER_SEC;
    }

    *out = result;
    return TU_OK;
}

TimeUtilsStatus time_utils_sleep_for(const struct timespec *duration)
{
    struct timespec remaining;
    struct timespec request;
    int rc;

    if (duration == NULL) {
        return TU_ERR_INVALID;
    }

    if (duration->tv_sec < 0 ||
        duration->tv_nsec < 0 ||
        duration->tv_nsec >= NS_PER_SEC) {
        return TU_ERR_INVALID;
    }

    if (duration->tv_sec == 0 && duration->tv_nsec == 0) {
        return TU_OK;
    }

    request = *duration;

    do {
        rc = clock_nanosleep(CLOCK_MONOTONIC, 0, &request, &remaining);
        if (rc == 0) {
            return TU_OK;
        }
        if (rc != EINTR) {
            return TU_ERR_SYSTEM;
        }
        request = remaining;
    } while (1);
}

TimeUtilsStatus time_utils_sleep_until(const struct timespec *deadline)
{
    struct timespec now;
    struct timespec remaining;

    if (deadline == NULL) {
        return TU_ERR_INVALID;
    }

    if (time_utils_now_mono(&now) != TU_OK) {
        return TU_ERR_SYSTEM;
    }

    if (!time_utils_ts_before(&now, deadline)) {
        return TU_OK;
    }

    if (time_utils_ts_sub(deadline, &now, &remaining) != TU_OK) {
        return TU_OK;
    }

    return time_utils_sleep_for(&remaining);
}

int time_utils_ts_before(const struct timespec *a, const struct timespec *b)
{
    if (a->tv_sec != b->tv_sec) {
        return a->tv_sec < b->tv_sec;
    }

    return a->tv_nsec < b->tv_nsec;
}
