/**
 * MIT License
 *
 * Copyright (c) 2020 kburger
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.github.kburger.fdp.validator.service;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Optional;
import java.util.function.Predicate;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.model.vocabulary.DCTERMS;
import org.eclipse.rdf4j.model.vocabulary.RDF4J;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParserRegistry;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.shacl.ShaclSail;
import org.eclipse.rdf4j.sail.shacl.ShaclSailValidationException;
import org.eclipse.rdf4j.sail.shacl.results.ValidationReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;

@SuppressWarnings("deprecation")
@Service
public class ValidatorService {
    public static final String PROF_NS = "http://www.w3.org/ns/dx/prof/";
    public static final IRI PROF_HASRESOURCE;
    public static final IRI PROF_HASROLE;
    public static final IRI PROF_HASARTIFACT;
    public static final IRI ROLE_VALIDATION;
    public static final IRI STANDARD_SHACL;
    
    public static final String ACCEPT_HEADER;
    
    private static final Logger logger = LoggerFactory.getLogger(ValidatorService.class);
    private static final ValueFactory FACTORY = SimpleValueFactory.getInstance();
    
    static {
        PROF_HASRESOURCE = FACTORY.createIRI(PROF_NS, "hasResource");
        PROF_HASROLE = FACTORY.createIRI(PROF_NS, "hasRole");
        PROF_HASARTIFACT = FACTORY.createIRI(PROF_NS, "hasArtifact");
        ROLE_VALIDATION = FACTORY.createIRI(PROF_NS + "role/", "Validation");
        STANDARD_SHACL = FACTORY.createIRI("https://www.w3.org/TR/shacl/");
        
        ACCEPT_HEADER = String.join(",", RDFFormat.getAcceptParams(RDFParserRegistry.getInstance().getKeys(), false, RDFFormat.TURTLE));
    }
    
    private HttpClient client;
    
    public ValidatorService() {
        client = HttpClient.newBuilder()
                .followRedirects(Redirect.ALWAYS)
                .build();
    }
    
    public Model validate(String resource) throws IOException {
        // resolve the resource
        var model = resolveResource(resource);
        
        // find a profile statement
        var profileIri = Models.getPropertyIRI(model, FACTORY.createIRI(resource), DCTERMS.CONFORMS_TO)
                .orElseThrow(() -> new IllegalStateException("Resource does not state its profile"));
        logger.info("Resolved profile {} for resource {}", profileIri, resource);
        
        // resolve the profile
        var artifact = resolveProfileArtifact(profileIri);
        if (artifact.isEmpty()) {
            throw new IllegalStateException("Profile does not define a SHACL validation artifact");
        }
        logger.info("Resolved validation artifact {} for profile {}", profileIri);
        
        // load the data
        var sail = new ShaclSail(new MemoryStore());
        sail.setUndefinedTargetValidatesAllSubjects(true);
        var repository = new SailRepository(sail);
        
        try (var connection = repository.getConnection()) {
            connection.begin();
            
            var shacl = resolveResource(artifact.orElseThrow().toString());
            connection.add(shacl, RDF4J.SHACL_SHAPE_GRAPH);
            
            connection.commit();
        } catch (RepositoryException e) {
            // shouldn't happen
            // TODO handle properly
        }
        
        // validate
        try (var connection = repository.getConnection()) {
            connection.begin();
            connection.add(model);
            connection.commit();
        } catch (RepositoryException e) {
            var cause = e.getCause();
            
            if (cause instanceof ShaclSailValidationException) {
                var report = ((ShaclSailValidationException)cause).validationReportAsModel();
                
                return report;
            }
        } finally {
            // cleanup
            repository.shutDown();
        }
        
        
        return new ValidationReport(true).asModel();
    }
    
    private Model resolveResource(String resource) throws IOException {
        var request = HttpRequest.newBuilder(URI.create(resource))
                .GET()
                .header(HttpHeaders.ACCEPT, ACCEPT_HEADER)
                .build();
        
        final HttpResponse<InputStream> response;
        try {
            response = client.send(request, BodyHandlers.ofInputStream());
        } catch (InterruptedException e) {
            throw new RuntimeException(e); // TODO
        }
        
        var format = response.headers()
                .firstValue(HttpHeaders.CONTENT_TYPE)
                .flatMap(contentType -> {
                    return RDFFormat.matchMIMEType(contentType, RDFParserRegistry.getInstance().getKeys());
                })
                .orElseThrow(() -> new RuntimeException("Could not find parser for resource"));
        
        return Rio.parse(response.body(), resource, format);
    }
    
    private Optional<IRI> resolveProfileArtifact(IRI profileIri) throws IOException {
        var profile = resolveResource(profileIri.toString());
        
        for (var profileResource : Models.getPropertyResources(profile, profileIri, PROF_HASRESOURCE)) {
            var resource = Models.getPropertyIRI(profile, profileResource, PROF_HASROLE)
                    .filter(Predicate.isEqual(ROLE_VALIDATION));
            
            if (resource.isEmpty()) {
                continue;
            }
            
            var standard = Models.getPropertyIRI(profile, profileResource, DCTERMS.CONFORMS_TO)
                    .filter(Predicate.isEqual(STANDARD_SHACL));
            
            if (standard.isEmpty()) {
                continue;
            }
            
            var artifact = Models.getPropertyIRI(profile, profileResource, PROF_HASARTIFACT);
            
            if (artifact.isPresent()) {
                return artifact;
            }
        }
        
        return Optional.empty();
    }
}
