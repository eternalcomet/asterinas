// SPDX-License-Identifier: MPL-2.0

#include "../../common/test.h"
#include <sys/epoll.h>
#include <unistd.h>

// Adding an epoll file to itself must fail with EINVAL, whether the epoll
// file is referred to by the same fd or by a duplicated one.
FN_TEST(epoll_add_self_disallowed)
{
	struct epoll_event ev = { .events = EPOLLIN, .data.u64 = 0 };
	int epfd, dup_epfd;

	epfd = TEST_SUCC(epoll_create1(0));
	dup_epfd = TEST_SUCC(dup(epfd));

	TEST_ERRNO(epoll_ctl(epfd, EPOLL_CTL_ADD, epfd, &ev), EINVAL);
	TEST_ERRNO(epoll_ctl(epfd, EPOLL_CTL_ADD, dup_epfd, &ev), EINVAL);
	TEST_ERRNO(epoll_ctl(dup_epfd, EPOLL_CTL_ADD, epfd, &ev), EINVAL);

	TEST_SUCC(close(epfd));
	TEST_SUCC(close(dup_epfd));
}
END_TEST()

// Two epoll files must not watch each other. A cycle deadlocks the system
// when a watched file becomes ready: the readiness notification travels
// through the epoll files synchronously and re-enters the ready list of the
// epoll file being notified, which is protected by a spin lock.
FN_TEST(epoll_cycle_of_two_disallowed)
{
	struct epoll_event ev = { .events = EPOLLIN, .data.u64 = 0 };
	int epfd_a, epfd_b;

	epfd_a = TEST_SUCC(epoll_create1(0));
	epfd_b = TEST_SUCC(epoll_create1(0));

	TEST_SUCC(epoll_ctl(epfd_a, EPOLL_CTL_ADD, epfd_b, &ev));
	TEST_ERRNO(epoll_ctl(epfd_b, EPOLL_CTL_ADD, epfd_a, &ev), ELOOP);

	TEST_SUCC(close(epfd_a));
	TEST_SUCC(close(epfd_b));
}
END_TEST()

// Cycles spanning more epoll files must also be rejected with ELOOP.
FN_TEST(epoll_cycle_of_three_disallowed)
{
	struct epoll_event ev = { .events = EPOLLIN, .data.u64 = 0 };
	int epfd_a, epfd_b, epfd_c;

	epfd_a = TEST_SUCC(epoll_create1(0));
	epfd_b = TEST_SUCC(epoll_create1(0));
	epfd_c = TEST_SUCC(epoll_create1(0));

	TEST_SUCC(epoll_ctl(epfd_a, EPOLL_CTL_ADD, epfd_b, &ev));
	TEST_SUCC(epoll_ctl(epfd_b, EPOLL_CTL_ADD, epfd_c, &ev));
	TEST_ERRNO(epoll_ctl(epfd_c, EPOLL_CTL_ADD, epfd_a, &ev), ELOOP);

	TEST_SUCC(close(epfd_a));
	TEST_SUCC(close(epfd_b));
	TEST_SUCC(close(epfd_c));
}
END_TEST()

// Watching an epoll file through another one, without creating a cycle,
// keeps working: the readiness of a file travels up the chain.
FN_TEST(epoll_nesting_without_cycle)
{
	int fildes[2];
	struct epoll_event ev = { .events = EPOLLIN, .data.u32 = 42 };
	int epfd_a, epfd_b;

	TEST_SUCC(pipe(fildes));
	epfd_a = TEST_SUCC(epoll_create1(0));
	epfd_b = TEST_SUCC(epoll_create1(0));

	TEST_SUCC(epoll_ctl(epfd_a, EPOLL_CTL_ADD, epfd_b, &ev));
	TEST_SUCC(epoll_ctl(epfd_b, EPOLL_CTL_ADD, fildes[0], &ev));

	TEST_RES(epoll_wait(epfd_a, &ev, 1, 0), _ret == 0);
	TEST_SUCC(write(fildes[1], "", 1));
	TEST_RES(epoll_wait(epfd_a, &ev, 1, 0),
		 _ret == 1 && ev.events == EPOLLIN && ev.data.u32 == 42);

	TEST_SUCC(close(epfd_a));
	TEST_SUCC(close(epfd_b));
	TEST_SUCC(close(fildes[0]));
	TEST_SUCC(close(fildes[1]));
}
END_TEST()

// Nesting more epoll files than the Linux limit must fail with ELOOP.
FN_TEST(epoll_nesting_too_deep_disallowed)
{
	struct epoll_event ev = { .events = EPOLLIN, .data.u64 = 0 };
	int epfds[6];

	for (int i = 0; i < 6; i++) {
		epfds[i] = TEST_SUCC(epoll_create1(0));
	}

	// A chain of five epoll files is allowed
	for (int i = 0; i < 4; i++) {
		TEST_SUCC(
			epoll_ctl(epfds[i], EPOLL_CTL_ADD, epfds[i + 1], &ev));
	}

	// But making the chain six epoll files long nests them too deeply
	TEST_ERRNO(epoll_ctl(epfds[4], EPOLL_CTL_ADD, epfds[5], &ev), ELOOP);

	for (int i = 0; i < 6; i++) {
		TEST_SUCC(close(epfds[i]));
	}
}
END_TEST()
