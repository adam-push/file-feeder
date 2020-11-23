package com.pushtechnology.utils.filefeeder;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class Tree<T> {

    private TreeNode<T> root;

    final class TreeNode<T> {
        String name;
        T data;
        TreeNode<T> parent;
        List<TreeNode<T>> children;

        TreeNode(String name, T data, TreeNode<T> parent) {
            this.name = name;
            this.data = data;
            this.parent = parent;
            this.children = new LinkedList<>();
        }

        public void setData(T data) {
            this.data = data;
        }
    }

    public Tree() {
        this.root = new TreeNode("/", null, null);
    }

    public TreeNode<T> add(String path, T data) {
        LinkedList<TreeNode<T>> nodes = makeNodes(path);
        TreeNode last = nodes.getLast();
        if(last != null) {
            last.setData(data);
        }
        return last;
    }

    public TreeNode<T> remove(String path) {
        LinkedList<TreeNode<T>> pathToNode = pathToNode(path);
        if(pathToNode.size() == 0) {
            return null;
        }

        TreeNode<T> last = pathToNode.getLast();
        if(last.parent == null) {
            // This is the root node
            root = null;
            return last;
        }

        if(last.parent.children.remove(last)) {
            return last;
        }
        else {
            return null;
        }
    }

    public TreeNode<T> get(String path) {
        LinkedList<TreeNode<T>> nodes = pathToNode(path);
        if(nodes.size() == 0) {
            return null;
        }
        return nodes.getLast();
    }

    public LinkedList<TreeNode<T>> getSiblings(TreeNode<T> node) {
        List<TreeNode<T>> siblings = node.parent.children;

        // Remove this node from the children, can't be your own sibling.
        if(siblings.size() > 1) {
            return new LinkedList<>(siblings
                    .stream()
                    .filter(n -> n.name != node.name)
                    .collect(Collectors.toList()));
        }
        return new LinkedList<TreeNode<T>>();
    }

    public LinkedList<TreeNode<T>> getSiblings(String path) {
        LinkedList<TreeNode<T>> nodes = pathToNode(path);
        if(nodes.size() > 0) {
            return getSiblings(nodes.getLast());
        }
        return new LinkedList<TreeNode<T>>();
    }

    private LinkedList<TreeNode<T>> makeNodes(String path) {
        LinkedList<TreeNode<T>> nodes = new LinkedList<>();
        String[] parts = path.split("/");

        TreeNode<T> currentNode = root;

        for(var part : parts) {

            // Look for part in children of current node
            boolean found = false;
            List<TreeNode<T>> children = currentNode.children;
            for(var node : children) {
                if(node.name.equals(part)) {
                    found = true;
                    currentNode = node;
                    nodes.add(node);
                    break;
                }
            }

            if(!found) {
                TreeNode<T> node = new TreeNode<>(part, null, currentNode);
                currentNode.children.add(node);
                currentNode = node;
                nodes.add(node);
            }
        }

        return nodes;
    }

    private LinkedList<TreeNode<T>> pathToNode(String path) {
        LinkedList<TreeNode<T>> nodes = new LinkedList<>();
        String[] parts = path.split("/");

        TreeNode<T> node = root;
        for(var part : parts) {
            var children = node.children;
            var found = false;
            for(var child : children) {
                if (child.name.equals(part)) {
                    found = true;
                    nodes.add(child);
                    node = child;
                    break;
                }
            }
            if(! found) {
                return new LinkedList<>();
            }
        }

        return nodes;
    }

    public LinkedList<TreeNode<T>> getNodesWithData() {
        return getNodesWithData(root);
    }

    public LinkedList<TreeNode<T>> getNodesWithData(TreeNode<T> node) {
        LinkedList<TreeNode<T>> nodes = new LinkedList<>();

        if(node.data != null) {
            nodes.add(node);
        }

        node.children.forEach(child -> {
           nodes.addAll(getNodesWithData(child));
        });

        return nodes;
    }

    public String getFullName(TreeNode<T> node) {
        while(node.parent != root) {
            return getFullName(node.parent) + "/" + node.name;
        }
        return node.name;
    }

    public void debug() {
        _debug(root, new LinkedList<String>());
    }
    public void _debug(TreeNode<T> node, List<String> path) {
        String str = String.join("/", path);
        System.out.println(str + " => " + node.data);
        node.children.forEach(child -> {
            path.add(child.name);
            _debug(child, path);
        });
    }
}
