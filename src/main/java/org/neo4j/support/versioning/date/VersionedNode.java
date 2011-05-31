package org.neo4j.support.versioning.date;

import java.util.Iterator;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Traverser;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.FilteringIterable;
import org.neo4j.helpers.collection.IterableWrapper;

public class VersionedNode implements Node {
    private final Node node;
    private final VersionContext versionContext;

    public VersionedNode(Node node, VersionContext versionContext) {
        this.node = node;
        this.versionContext = versionContext;
    }

    @Override
    public long getId() {
        return node.getId();
    }

    @Override
    public void delete() {
        versionContext.deleteNode(node);
    }

    @Override
    public Iterable<Relationship> getRelationships() {
        return getValidRelationships(node.getRelationships());
    }

    @Override
    public Iterable<Relationship> getRelationships(Direction dir) {
        return getValidRelationships(node.getRelationships(dir));
    }

    @Override
    public Iterable<Relationship> getRelationships(RelationshipType... types) {
        return getValidRelationships(node.getRelationships(types));
    }

    @Override
    public Iterable<Relationship> getRelationships(RelationshipType type, Direction dir) {
        return getValidRelationships(node.getRelationships(type, dir));
    }

    private Iterable<Relationship> getValidRelationships(Iterable<Relationship> relationships) {
        return new IterableWrapper<Relationship, Relationship>(new FilteringIterable<Relationship>(relationships,
                new Predicate<Relationship>() {
                    @Override
                    public boolean accept(Relationship item) {
                        boolean valid = versionContext.hasValidVersion(item);
                        System.out.println("Inspecting rel: " + item + " (valid=" + valid + ")");
                        return valid;
                    }
                })) {
            @Override
            protected Relationship underlyingObjectToObject(Relationship object) {
                System.out.println("wrapping rel: " + object);
                return new VersionedRelationship(object, versionContext);
            }
        };
    }

    @Override
    public boolean hasRelationship() {
        return getRelationships().iterator().hasNext();
    }

    @Override
    public boolean hasRelationship(Direction dir) {
        return getRelationships(dir).iterator().hasNext();
    }

    @Override
    public boolean hasRelationship(RelationshipType... types) {
        return getRelationships(types).iterator().hasNext();
    }

    @Override
    public boolean hasRelationship(RelationshipType type, Direction dir) {
        return getRelationships(type, dir).iterator().hasNext();
    }

    @Override
    public Relationship getSingleRelationship(RelationshipType type, Direction dir) {
        Iterator<Relationship> iter = getRelationships(type, dir).iterator();
        if (!iter.hasNext()) {
            return null;
        }
        Relationship single = iter.next();
        if (iter.hasNext())
            throw new NotFoundException("More than one relationship[" + type + ", " + dir + "] found for " + this
                    + " in " + versionContext);
        return single;
    }

    @Override
    public Relationship createRelationshipTo(Node otherNode, RelationshipType type) {
        return new VersionedRelationship(node.createRelationshipTo(otherNode, type), versionContext);
    }

    @Override
    public Traverser traverse(Traverser.Order traversalOrder, StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator, RelationshipType relationshipType, Direction direction) {
        throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public Traverser traverse(Traverser.Order traversalOrder, StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator, RelationshipType firstRelationshipType, Direction firstDirection,
            RelationshipType secondRelationshipType, Direction secondDirection) {
        throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public Traverser traverse(Traverser.Order traversalOrder, StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator, Object... relationshipTypesAndDirections) {
        throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public GraphDatabaseService getGraphDatabase() {
        return node.getGraphDatabase();
    }

    @Override
    public boolean hasProperty(String key) {
        return versionContext.hasProperty(node, key);
    }

    @Override
    public Object getProperty(String key) {
        return versionContext.getProperty(node, key);
    }

    @Override
    public Object getProperty(String key, Object defaultValue) {
        return versionContext.getProperty(node, key, defaultValue);
    }

    @Override
    public void setProperty(String key, Object value) {
        node.setProperty(key, value);
    }

    @Override
    public Object removeProperty(String key) {
        return node.removeProperty(key);
    }

    @Override
    public Iterable<String> getPropertyKeys() {
        return versionContext.getPropertyKeys(node);
    }

    @Override
    public Iterable<Object> getPropertyValues() {
        return versionContext.getPropertyValues(node);
    }

    @Override
    public int hashCode() {
        return node.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return node.equals(obj);
    }

    @Override
    public Iterable<Relationship> getRelationships(Direction direction, RelationshipType... types) {
        return node.getRelationships(direction, types);
    }

    @Override
    public boolean hasRelationship(Direction direction, RelationshipType... types) {
        return node.hasRelationship(direction, types);
    }
}
