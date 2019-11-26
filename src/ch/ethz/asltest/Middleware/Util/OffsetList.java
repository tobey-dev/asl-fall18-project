package ch.ethz.asltest.Middleware.Util;


import ch.ethz.asltest.Middleware.Log.Log;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/*
    This is a wrapper over a list, which allows to set the starting index before iterating over the list's content.
    WorkerThreads hold a reference to an OffsetList of ServerConnections, to, once having set the first server to write
    to with round robin, iterate through the remaining servers in order (with looping around) to ensure that not all
    WorkerThreads write to the same servers at the same time to avoid congestion.
    This class is NOT thread safe.
 */

public class OffsetList<T>implements Iterable<T>{

    private List<T> list;
    private OffsetListIterator<T> iterator;
    private int offset;

    public OffsetList(){
        list = new ArrayList<>();
        iterator = new OffsetListIterator<>(this);
    }

    public void clear(T element){
        list.clear();
        setOffset(0);
    }

    public void add(T e){
        list.add(e);
    }

    public int size(){
        return list.size();
    }

    public T get(int index){
        return list.get(index);
    }

    public int indexOf(T e){
        return list.indexOf(e);
    }

    void remove(Object o){
        list.remove(o);
    }

    public int setOffset(int offset) throws IndexOutOfBoundsException{
        int previousOffset = this.offset;

        if (offset < 0){
            throw new IndexOutOfBoundsException("[OffsetList] Offset is out of bounds");
        }

        if ((list.size() > 0) && (offset > (list.size() - 1))){
            throw new IndexOutOfBoundsException("[OffsetList] Offset is out of bounds");
        } else if (list.size() == 0 && offset != 0){
            throw new IndexOutOfBoundsException("[OffsetList] Offset is out of bounds");
        }

        this.offset = offset;
        iterator.setOffset(offset);
        return previousOffset;
    }

    public int getOffset(){
        return offset;
    }

    @Override
    public Iterator<T> iterator() {
        iterator.reset();
        return iterator;
    }

    List<T> getList(){
        return list;
    }

    public static class OffsetListIterator<T> implements Iterator<T>{

        private OffsetList<T> offsetList;
        int offset;
        int currentIndex;

        T lastElement;

        boolean hasNext;

        OffsetListIterator(OffsetList<T> offsetList){
            this.offsetList = offsetList;
        }

        void setOffset(int offset){
            this.offset = offset;
        }

        void reset(){
            currentIndex = offset;
            if (offsetList.size() > 0){
                hasNext = true;
            }
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public T next() {
            T value = offsetList.get(currentIndex);
            currentIndex = (currentIndex + 1) % offsetList.size();
            if (currentIndex == offset){
                hasNext = false;
            }
            lastElement = value;
            return value;
        }

        @Override
        public void remove() {
            offsetList.remove(lastElement);
            currentIndex--;
        }
    }

}
