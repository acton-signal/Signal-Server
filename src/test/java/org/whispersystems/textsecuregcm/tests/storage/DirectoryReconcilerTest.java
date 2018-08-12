package org.whispersystems.textsecuregcm.tests.storage;

import com.google.common.base.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationRequest;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DirectoryReconciler;
import org.whispersystems.textsecuregcm.storage.DirectoryReconciliationCache;
import org.whispersystems.textsecuregcm.storage.DirectoryReconciliationClient;
import org.whispersystems.textsecuregcm.tests.util.SynchronousExecutorService;

import javax.ws.rs.core.Response;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class DirectoryReconcilerTest {

  private static final String VALID_NUMBER    = "valid";
  private static final String INACTIVE_NUMBER = "inactive";

  private static final long ACCOUNT_COUNT = 0L;

  private final Account                       account                       = mock(Account.class);
  private final Account                       inactiveAccount               = mock(Account.class);
  private final AccountsManager               accountsManager               = mock(AccountsManager.class);
  private final DirectoryReconciliationClient reconciliationClient          = mock(DirectoryReconciliationClient.class);
  private final DirectoryReconciliationCache  reconciliationCache           = mock(DirectoryReconciliationCache.class);

  private final Response successResponse  = mockResponse(200);
  private final Response notFoundResponse = mockResponse(404);

  @Before
  public void setup() {
    when(account.getNumber()).thenReturn(VALID_NUMBER);
    when(account.isActive()).thenReturn(true);
    when(inactiveAccount.getNumber()).thenReturn(INACTIVE_NUMBER);
    when(inactiveAccount.isActive()).thenReturn(false);

    when(accountsManager.getAll(eq(0), anyInt())).thenReturn(Arrays.asList(account, inactiveAccount));
    when(accountsManager.getAllFrom(eq(VALID_NUMBER), anyInt())).thenReturn(Arrays.asList(inactiveAccount));
    when(accountsManager.getAllFrom(eq(INACTIVE_NUMBER), anyInt())).thenReturn(Collections.emptyList());
    when(accountsManager.getCount()).thenReturn(ACCOUNT_COUNT);

    when(reconciliationClient.sendChunk(any(), any())).thenReturn(successResponse);

    when(reconciliationCache.getLastNumber()).thenReturn(Optional.absent());
    when(reconciliationCache.getCachedAccountCount()).thenReturn(Optional.of(ACCOUNT_COUNT));
    when(reconciliationCache.lockActiveWorker(any(), anyLong())).thenReturn(true);
    when(reconciliationCache.isAccelerated()).thenReturn(false);
    when(reconciliationCache.getWorkerCount(anyLong())).thenReturn(0L);
  }

  private static Response mockResponse(int responseStatus) {
    Response               response          = mock(Response.class);
    Response.StatusType    statusType        = mock(Response.StatusType.class);

    when(response.getStatus()).thenReturn(responseStatus);
    when(response.getStatusInfo()).thenReturn(statusType);

    when(statusType.getStatusCode()).thenReturn(responseStatus);
    when(statusType.getFamily()).thenReturn(Response.Status.Family.familyOf(responseStatus));

    return response;
  }

  @Test
  public void testValid() {
    when(reconciliationCache.getCachedAccountCount()).thenReturn(Optional.absent());

    DirectoryReconciler directoryReconciler = new DirectoryReconciler(reconciliationClient, reconciliationCache, accountsManager);
    directoryReconciler.start(new SynchronousExecutorService());
    directoryReconciler.stop();

    verify(accountsManager, times(1)).getAll(eq(0), anyInt());
    verify(accountsManager, times(1)).getCount();

    ArgumentCaptor<DirectoryReconciliationRequest> request = ArgumentCaptor.forClass(DirectoryReconciliationRequest.class);
    verify(reconciliationClient, times(1)).sendChunk(eq(Optional.absent()), request.capture());

    assertThat(request.getValue().getToNumber()).isEqualTo(INACTIVE_NUMBER);
    assertThat(request.getValue().getNumbers()).isEqualTo(Arrays.asList(VALID_NUMBER));

    verify(reconciliationCache, times(1)).cleanUpWorkerSet(anyLong());
    verify(reconciliationCache, times(1)).joinWorkerSet(any());
    verify(reconciliationCache, times(1)).leaveWorkerSet(any());
    verify(reconciliationCache, times(1)).getCachedAccountCount();
    verify(reconciliationCache, times(1)).setCachedAccountCount(eq(ACCOUNT_COUNT));
    verify(reconciliationCache, times(1)).getLastNumber();
    verify(reconciliationCache, times(1)).getWorkerCount(anyLong());
    verify(reconciliationCache, times(1)).setLastNumber(eq(Optional.of(INACTIVE_NUMBER)));
    verify(reconciliationCache, times(1)).isAccelerated();
    verify(reconciliationCache, times(1)).lockActiveWorker(any(), anyLong());
    verify(reconciliationCache, times(1)).unlockActiveWorker(any());

    verifyNoMoreInteractions(accountsManager);
    verifyNoMoreInteractions(reconciliationClient);
    verifyNoMoreInteractions(reconciliationCache);
  }

  @Test
  public void testInProgress() {
    when(reconciliationCache.getLastNumber()).thenReturn(Optional.of(VALID_NUMBER));

    DirectoryReconciler directoryReconciler = new DirectoryReconciler(reconciliationClient, reconciliationCache, accountsManager);
    directoryReconciler.start(new SynchronousExecutorService());
    directoryReconciler.stop();

    verify(accountsManager, times(1)).getAllFrom(eq(VALID_NUMBER), anyInt());

    ArgumentCaptor<DirectoryReconciliationRequest> request = ArgumentCaptor.forClass(DirectoryReconciliationRequest.class);
    verify(reconciliationClient, times(1)).sendChunk(eq(Optional.of(VALID_NUMBER)), request.capture());

    assertThat(request.getValue().getToNumber()).isEqualTo(INACTIVE_NUMBER);
    assertThat(request.getValue().getNumbers()).isEqualTo(Collections.emptyList());

    verify(reconciliationCache, times(1)).cleanUpWorkerSet(anyLong());
    verify(reconciliationCache, times(1)).joinWorkerSet(any());
    verify(reconciliationCache, times(1)).leaveWorkerSet(any());
    verify(reconciliationCache, times(1)).getCachedAccountCount();
    verify(reconciliationCache, times(1)).getLastNumber();
    verify(reconciliationCache, times(1)).getWorkerCount(anyLong());
    verify(reconciliationCache, times(1)).setLastNumber(eq(Optional.of(INACTIVE_NUMBER)));
    verify(reconciliationCache, times(1)).isAccelerated();
    verify(reconciliationCache, times(1)).lockActiveWorker(any(), anyLong());
    verify(reconciliationCache, times(1)).unlockActiveWorker(any());

    verifyNoMoreInteractions(accountsManager);
    verifyNoMoreInteractions(reconciliationClient);
    verifyNoMoreInteractions(reconciliationCache);
  }

  @Test
  public void testLastChunk() {
    when(reconciliationCache.getLastNumber()).thenReturn(Optional.of(INACTIVE_NUMBER));

    DirectoryReconciler directoryReconciler = new DirectoryReconciler(reconciliationClient, reconciliationCache, accountsManager);
    directoryReconciler.start(new SynchronousExecutorService());
    directoryReconciler.stop();

    verify(accountsManager, times(1)).getAllFrom(eq(INACTIVE_NUMBER), anyInt());

    ArgumentCaptor<DirectoryReconciliationRequest> request = ArgumentCaptor.forClass(DirectoryReconciliationRequest.class);
    verify(reconciliationClient, times(1)).sendChunk(eq(Optional.of(INACTIVE_NUMBER)), request.capture());

    assertThat(request.getValue().getToNumber()).isNull();
    assertThat(request.getValue().getNumbers()).isEqualTo(Collections.emptyList());

    verify(reconciliationCache, times(1)).cleanUpWorkerSet(anyLong());
    verify(reconciliationCache, times(1)).joinWorkerSet(any());
    verify(reconciliationCache, times(1)).leaveWorkerSet(any());
    verify(reconciliationCache, times(1)).getCachedAccountCount();
    verify(reconciliationCache, times(1)).getLastNumber();
    verify(reconciliationCache, times(1)).getWorkerCount(anyLong());
    verify(reconciliationCache, times(1)).setLastNumber(eq(Optional.absent()));
    verify(reconciliationCache, times(1)).clearAccelerate();
    verify(reconciliationCache, times(1)).isAccelerated();
    verify(reconciliationCache, times(1)).lockActiveWorker(any(), anyLong());
    verify(reconciliationCache, times(1)).unlockActiveWorker(any());

    verifyNoMoreInteractions(accountsManager);
    verifyNoMoreInteractions(reconciliationClient);
    verifyNoMoreInteractions(reconciliationCache);
  }

  @Test
  public void testNotFound() {
    when(reconciliationClient.sendChunk(any(), any())).thenReturn(notFoundResponse);

    DirectoryReconciler directoryReconciler = new DirectoryReconciler(reconciliationClient, reconciliationCache, accountsManager);
    directoryReconciler.start(new SynchronousExecutorService());
    directoryReconciler.stop();

    verify(accountsManager, times(1)).getAll(eq(0), anyInt());

    ArgumentCaptor<DirectoryReconciliationRequest> request = ArgumentCaptor.forClass(DirectoryReconciliationRequest.class);
    verify(reconciliationClient, times(1)).sendChunk(eq(Optional.absent()), request.capture());

    assertThat(request.getValue().getToNumber()).isEqualTo(INACTIVE_NUMBER);
    assertThat(request.getValue().getNumbers()).isEqualTo(Arrays.asList(VALID_NUMBER));

    verify(reconciliationCache, times(1)).cleanUpWorkerSet(anyLong());
    verify(reconciliationCache, times(1)).joinWorkerSet(any());
    verify(reconciliationCache, times(1)).leaveWorkerSet(any());
    verify(reconciliationCache, times(1)).getCachedAccountCount();
    verify(reconciliationCache, times(1)).getLastNumber();
    verify(reconciliationCache, times(1)).getWorkerCount(anyLong());
    verify(reconciliationCache, times(1)).setLastNumber(eq(Optional.absent()));
    verify(reconciliationCache, times(1)).clearAccelerate();
    verify(reconciliationCache, times(1)).isAccelerated();
    verify(reconciliationCache, times(1)).lockActiveWorker(any(), anyLong());
    verify(reconciliationCache, times(1)).unlockActiveWorker(any());

    verifyNoMoreInteractions(accountsManager);
    verifyNoMoreInteractions(reconciliationClient);
    verifyNoMoreInteractions(reconciliationCache);
  }

}
