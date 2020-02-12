package com.github.hermannpencole.nifi.config.service;

import com.github.hermannpencole.nifi.config.model.ConfigException;
import com.github.hermannpencole.nifi.swagger.ApiException;
import com.github.hermannpencole.nifi.swagger.client.ProcessorsApi;
import com.github.hermannpencole.nifi.swagger.client.model.ProcessorDTO;
import com.github.hermannpencole.nifi.swagger.client.model.ProcessorEntity;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

/**
 * API tests for AccessApi
 */
@RunWith(MockitoJUnitRunner.class)
public class ProcessorServiceTest {

    @Mock
    private ProcessorsApi processorsApiMock;

    @Test
    public void setStateAlreadyTest() throws ApiException {
//        Injector injector = Guice.createInjector(new AbstractModule() {
//            protected void configure() {
//                bind(ProcessorsApi.class).toInstance(processorsApiMock);
//                bind(Integer.class).annotatedWith(Names.named("timeout")).toInstance(1);
//                bind(Integer.class).annotatedWith(Names.named("interval")).toInstance(1);
//                bind(Boolean.class).annotatedWith(Names.named("forceMode")).toInstance(false);
//            }
//        });
        ProcessorService processorService = null;//injector.getInstance(ProcessorService.class);
        ProcessorEntity processor = TestUtils.createProcessorEntity("id", "name");
        processor.getComponent().setState(ProcessorDTO.StateEnum.RUNNING);
        processorService.setState(processor, ProcessorDTO.StateEnum.RUNNING);
        verify(processorsApiMock, never()).updateProcessor(anyString(), anyObject());
    }

    @Test
    public void setStateTest() throws ApiException {
//        Injector injector = Guice.createInjector(new AbstractModule() {
//            protected void configure() {
//                bind(ProcessorsApi.class).toInstance(processorsApiMock);
//                bind(Integer.class).annotatedWith(Names.named("timeout")).toInstance(1);
//                bind(Integer.class).annotatedWith(Names.named("interval")).toInstance(1);
//                bind(Boolean.class).annotatedWith(Names.named("forceMode")).toInstance(false);
//            }
//        });
        ProcessorService processorService = null;//injector.getInstance(ProcessorService.class);
        ProcessorEntity processor = TestUtils.createProcessorEntity("id", "name");
        processor.getComponent().setState(ProcessorDTO.StateEnum.STOPPED);

        ProcessorEntity processorResponse = TestUtils.createProcessorEntity("id", "name");
        processorResponse.getComponent().setState(ProcessorDTO.StateEnum.RUNNING);
        when(processorsApiMock.updateProcessor(eq("id"), any() )).thenReturn(processorResponse);
        when(processorsApiMock.getProcessor(eq("id"))).thenReturn(processorResponse);

        processorService.setState(processor, ProcessorDTO.StateEnum.RUNNING);
        ArgumentCaptor<ProcessorEntity> processorEntity = ArgumentCaptor.forClass(ProcessorEntity.class);
        verify(processorsApiMock).updateProcessor(eq("id"), processorEntity.capture());
        assertEquals("id", processorEntity.getValue().getComponent().getId());
        assertEquals( ProcessorDTO.StateEnum.RUNNING, processorEntity.getValue().getComponent().getState());
    }

    @Test(expected = ConfigException.class)
    public void setStateExceptionTest() throws ApiException {
//        Injector injector = Guice.createInjector(new AbstractModule() {
//            protected void configure() {
//                bind(ProcessorsApi.class).toInstance(processorsApiMock);
//                bind(Integer.class).annotatedWith(Names.named("timeout")).toInstance(1);
//                bind(Integer.class).annotatedWith(Names.named("interval")).toInstance(1);
//                bind(Boolean.class).annotatedWith(Names.named("forceMode")).toInstance(false);
//            }
//        });
        ProcessorService processorService = null;//injector.getInstance(ProcessorService.class);
        ProcessorEntity processor = TestUtils.createProcessorEntity("id", "name");
        processor.getComponent().setState(ProcessorDTO.StateEnum.STOPPED);

        ProcessorEntity processorResponse = TestUtils.createProcessorEntity("id", "name");
        processorResponse.getComponent().setState(ProcessorDTO.StateEnum.RUNNING);
        when(processorsApiMock.updateProcessor(eq("id"), any() )).thenThrow(new ApiException());
        when(processorsApiMock.getProcessor(eq("id") )).thenReturn(processorResponse);

        processorService.setState(processor, ProcessorDTO.StateEnum.RUNNING);
    }

}