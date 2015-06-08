package org.openrdf.spring;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

@Transactional(value = "transactionManager")
public class TransactionBean {
	// Retrieve the connection factory to access the repository
    
	public TransactionBean() {
	}
	
	@Autowired
	protected SesameConnectionFactory repositoryManagerConnectionFactory;

	public void addTestData(SesameConnectionFactory factory)
			throws Exception {
		this.addData(factory);
	}
	
	@Transactional("transactionManager")
	public void addData(SesameConnectionFactory factory)
			throws Exception {
		RepositoryConnection connection = null;
		if (repositoryManagerConnectionFactory == null) {
			// Acquire the connection
			connection = factory.getConnection();
		} else {
			connection = repositoryManagerConnectionFactory.getConnection();
		}
			
        ValueFactory f = ValueFactoryImpl.getInstance();
        URI a = f.createURI("http://example.com/a");
        URI b = f.createURI("http://example.com/b");
        URI c = f.createURI("http://example.com/c");

        connection.add(a, b, c);
	}

	public SesameConnectionFactory getRepositoryConnectionFactory() {
		return repositoryManagerConnectionFactory;
	}

	public void setRepositoryConnectionFactory(
			SesameConnectionFactory repositoryManagerConnectionFactory) {
		this.repositoryManagerConnectionFactory = repositoryManagerConnectionFactory;
	}

	@Transactional(value="transactionManager", rollbackFor=RepositoryException.class)
	public void addDataFail()
			throws Exception {
		// Acquire the connection
		RepositoryConnection connection = repositoryManagerConnectionFactory.getConnection();

        ValueFactory f = ValueFactoryImpl.getInstance();
        URI a = f.createURI("http://example.com/a");
        URI b = f.createURI("http://example.com/b");
        URI c = f.createURI("http://example.com/c");

        connection.add(a, b, c);
        
        throw new RepositoryException();
	}
	
	public boolean getDataHasNext() throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		// Acquire the connection
		RepositoryConnection connection = repositoryManagerConnectionFactory.getConnection();
        final TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT ?s ?o WHERE { ?s <http://example.com/b> ?o . }");
        TupleQueryResult result = tupleQuery.evaluate();
        if (result.hasNext()) {
        	return true;
        } else
        	return false;
	}
}