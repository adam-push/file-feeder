package com.pushtechnology.utils.filefeeder;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedList;

class TreeTest {

    Tree<String> tree;

    @BeforeEach
    void setUp() {
        tree = new Tree<>();
        tree.add("foo/bar", "apple");
        tree.add("foo/bar/baz", "banana");
        tree.add("foo/bar/qux", "cucumber");
    }

    @org.junit.jupiter.api.Test
    void add() {
        assert(tree.get("foo/bar").data == "apple");
        assert(tree.get("foo/bar/baz").data == "banana");
        assert(tree.get("foo/bar/qux").data == "cucumber");

        tree.debug();
    }

    @org.junit.jupiter.api.Test
    void remove() {
        Tree.TreeNode node = tree.remove("foo/bar/baz");
        assert(((String)node.data).equals("banana"));
        tree.debug();

        node = tree.remove("foo/bar");
        assert(((String)node.data).equals("apple"));
        tree.debug();
    }

    @Test
    void getFullName() {
        System.out.println(tree.getFullName(tree.get("foo/bar/baz")));
    }

    @org.junit.jupiter.api.Test
    void getSiblings() {
        System.out.println("Siblings of foo/bar/baz");
        LinkedList<Tree<String>.TreeNode<String>> siblings = tree.getSiblings("foo/bar/baz");
        for(var s : siblings) {
            System.out.println("" + s.name + " = " + s.data);
        }
    }

    @Test
    void getNodesWithData() {
        System.out.println("Nodes with data");
        LinkedList<Tree<String>.TreeNode<String>> nodesWithData = tree.getNodesWithData();
        for(var n : nodesWithData) {
            System.out.println(n.name + " => " + n.data);
        }
    }

    @Test
    void shuffleTest() {
        System.out.println("Shuffle nodes with data");
        System.out.println("Before");
        LinkedList<Tree<String>.TreeNode<String>> nodesWithData = tree.getNodesWithData();
        for(var n : nodesWithData) {
            System.out.println(n.name + " => " + n.data);
        }

        System.out.println("After");
        Collections.shuffle(nodesWithData);
        for(var n : nodesWithData) {
            System.out.println(n.name + " => " + n.data);
        }

        tree.debug();

    }
}