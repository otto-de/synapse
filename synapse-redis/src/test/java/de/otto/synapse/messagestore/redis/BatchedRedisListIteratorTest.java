package de.otto.synapse.messagestore.redis;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.data.redis.core.BoundListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.function.Function.identity;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class BatchedRedisListIteratorTest {

    @Mock
    private StringRedisTemplate redisTemplate;
    @Mock
    private BoundListOperations<String, String> ops;

    @Before
    public void before() {
        initMocks(this);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectEmptyListName() {
        new BatchedRedisListIterator<>(redisTemplate, identity(), "", 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectIllegalBatchSize() {
        new BatchedRedisListIterator<>(redisTemplate, identity(), "foo", 0);
    }

    @Test
    public void shouldHaveNextElement() {
        // given
        when(ops.range(0, 0)).thenReturn(singletonList("foo"));
        when(redisTemplate.boundListOps(any(String.class))).thenReturn(ops);
        final Iterator<String> testee = new BatchedRedisListIterator<>(redisTemplate, identity(), "some-list", 1);
        // when
        final boolean hasNext = testee.hasNext();
        // then
        assertTrue(hasNext);
    }

    @Test
    public void shouldHaveNoNextElementInEmptyIter() {
        // given
        when(ops.range(0, 0)).thenReturn(emptyList());
        when(redisTemplate.boundListOps(any(String.class))).thenReturn(ops);
        final Iterator<String> testee = new BatchedRedisListIterator<>(redisTemplate, identity(), "some-list", 1);
        // when
        final boolean hasNext = testee.hasNext();
        // then
        assertFalse(hasNext);
    }

    @Test
    public void shouldHaveNoNextElementAfterLast() {
        // given
        when(ops.range(0, 1)).thenReturn(singletonList("foo"));
        when(redisTemplate.boundListOps(any(String.class))).thenReturn(ops);
        final Iterator<String> testee = new BatchedRedisListIterator<>(redisTemplate, identity(), "some-list", 2);
        // when
        testee.next();
        final boolean hasNext = testee.hasNext();
        // then
        assertFalse(hasNext);
    }

    @Test
    public void shouldNotFetchNextBatchIfFirstIsOnlyPartlyFilled() {
        // given
        when(ops.range(0, 1)).thenReturn(singletonList("foo"));
        when(redisTemplate.boundListOps(any(String.class))).thenReturn(ops);
        final Iterator<String> testee = new BatchedRedisListIterator<>(redisTemplate, identity(), "some-list", 2);
        // when
        testee.next();
        testee.hasNext();
        verify(ops, times(1)).range(0, 1);
        verifyNoMoreInteractions(ops);
    }

    @Test
    public void shouldReturnNext() {
        // given
        when(ops.range(0, 0)).thenReturn(singletonList("foo"));
        when(redisTemplate.boundListOps(any(String.class))).thenReturn(ops);
        final Iterator<String> testee = new BatchedRedisListIterator<>(redisTemplate, identity(), "some-list", 1);
        // when
        final String value = testee.next();
        // then
        assertEquals(value, "foo");
    }

    @Test
    public void shouldTransformNext() {
        // given
        when(ops.range(0, 0)).thenReturn(singletonList("foo"));
        when(redisTemplate.boundListOps(any(String.class))).thenReturn(ops);
        final Iterator<String> testee = new BatchedRedisListIterator<>(redisTemplate, (s)->"bar", "some-list", 1);
        // when
        final String value = testee.next();
        // then
        assertEquals(value, "bar");
    }

    @Test(expected = NoSuchElementException.class)
    public void shouldFailWithNoSuchElementExceptionInEmptyBatch() {
        // given
        when(ops.range(0, 0)).thenReturn(emptyList());
        when(redisTemplate.boundListOps(any(String.class))).thenReturn(ops);
        final Iterator<String> testee = new BatchedRedisListIterator<>(redisTemplate, identity(), "some-list", 1);
        // when
        testee.next();
    }

    @Test(expected = NoSuchElementException.class)
    public void shouldFailWithNoSuchElementExceptionAfterLastElement() {
        // given
        when(ops.range(0, 1)).thenReturn(singletonList("foo"));
        when(redisTemplate.boundListOps(any(String.class))).thenReturn(ops);
        final Iterator<String> testee = new BatchedRedisListIterator<>(redisTemplate, identity(), "some-list", 2);
        // when
        testee.next();
        testee.next();
    }

    @Test
    public void shouldFetchNextBatch() {
        // given
        when(ops.range(0, 1)).thenReturn(asList("foo", "bar"));
        when(ops.range(2, 3)).thenReturn(singletonList("foobar"));
        when(redisTemplate.boundListOps(any(String.class))).thenReturn(ops);
        final Iterator<String> testee = new BatchedRedisListIterator<>(redisTemplate, identity(), "some-list", 2);
        // when
        assertEquals(testee.next(), "foo");
        assertEquals(testee.next(), "bar");
        assertEquals(testee.next(), "foobar");
        assertFalse(testee.hasNext());
        verify(ops, times(1)).range(0, 1);
        verify(ops, times(1)).range(2, 3);
        verifyNoMoreInteractions(ops);
    }

    @Test
    public void shouldHaveNoNextElementAfterFirstBatchIfSecondBatchIsEmpty() {
        // given
        when(ops.range(0, 1)).thenReturn(asList("foo", "bar"));
        when(ops.range(2, 3)).thenReturn(emptyList());
        when(redisTemplate.boundListOps(any(String.class))).thenReturn(ops);
        final Iterator<String> testee = new BatchedRedisListIterator<>(redisTemplate, identity(), "some-list", 2);
        // when
        assertEquals(testee.next(), "foo");
        assertEquals(testee.next(), "bar");
        assertFalse(testee.hasNext());
        verify(ops, times(1)).range(0, 1);
        verify(ops, times(1)).range(2, 3);
        verifyNoMoreInteractions(ops);
    }


}