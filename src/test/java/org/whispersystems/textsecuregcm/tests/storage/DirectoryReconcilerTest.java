package org.whispersystems.textsecuregcm.tests.storage;

import com.google.common.base.Optional;
import org.junit.Before;
import org.junit.Test;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DirectoryReconciler;
import org.whispersystems.textsecuregcm.storage.DirectoryReconciliationCache;
import org.whispersystems.textsecuregcm.storage.DirectoryReconciliationClient;
import org.whispersystems.textsecuregcm.tests.util.SynchronousExecutorService;

import javax.ws.rs.core.Response;

import java.util.Arrays;
import java.util.Collections;

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

  private static final String VALID_NUMBER = "valid";

  private static final long ACCOUNT_COUNT = 0L;

  private final Account                       account                       = mock(Account.class);
  private final AccountsManager               accountsManager               = mock(AccountsManager.class);
  private final AccountsManager               notFoundAccountsManager       = mock(AccountsManager.class);
  private final DirectoryReconciliationClient reconciliationClient          = mock(DirectoryReconciliationClient.class);
  private final DirectoryReconciliationClient notFoundReconciliationClient  = mock(DirectoryReconciliationClient.class);
  private final DirectoryReconciliationCache  reconciliationCache           = mock(DirectoryReconciliationCache.class);
  private final DirectoryReconciliationCache  inProgressReconciliationCache = mock(DirectoryReconciliationCache.class);

  private final Response successResponse  = mockResponse(200);
  private final Response notFoundResponse = mockResponse(404);

  @Before
  public void setup() {
    when(account.getNumber()).thenReturn(VALID_NUMBER);
    when(accountsManager.getAll(eq(0), anyInt())).thenReturn(Arrays.asList(account));
    when(accountsManager.getAllFrom(eq(VALID_NUMBER), anyInt())).thenReturn(Collections.emptyList());
    when(accountsManager.getCount()).thenReturn(ACCOUNT_COUNT);

    when(notFoundAccountsManager.getAll(eq(0), anyInt())).thenReturn(Arrays.asList(account));
    when(notFoundAccountsManager.getCount()).thenReturn(ACCOUNT_COUNT);

    when(reconciliationClient.sendChunk(any(), any())).thenReturn(successResponse);

    when(notFoundReconciliationClient.sendChunk(any(), any())).thenReturn(notFoundResponse);

    when(reconciliationCache.getCachedAccountCount()).thenReturn(Optional.absent());
    when(reconciliationCache.getLastNumber()).thenReturn(Optional.absent());
    when(reconciliationCache.lockActiveWorker(any(), anyLong())).thenReturn(true);
    when(reconciliationCache.isAccelerated()).thenReturn(false);
    when(reconciliationCache.getWorkerCount(anyLong())).thenReturn(0L);

    when(inProgressReconciliationCache.getCachedAccountCount()).thenReturn(Optional.of(ACCOUNT_COUNT));
    when(inProgressReconciliationCache.getLastNumber()).thenReturn(Optional.of(VALID_NUMBER));
    when(inProgressReconciliationCache.lockActiveWorker(any(), anyLong())).thenReturn(true);
    when(inProgressReconciliationCache.isAccelerated()).thenReturn(false);
    when(inProgressReconciliationCache.getWorkerCount(anyLong())).thenReturn(1L);
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
    DirectoryReconciler directoryReconciler = new DirectoryReconciler(reconciliationClient, reconciliationCache, accountsManager);
    directoryReconciler.start(new SynchronousExecutorService());
    directoryReconciler.stop();

    verify(accountsManager, times(1)).getAll(eq(0), anyInt());
    verify(accountsManager, times(1)).getCount();

    verify(reconciliationClient, times(1)).sendChunk(eq(Optional.absent()), any());

    verify(reconciliationCache, times(1)).cleanUpWorkerSet(anyLong());
    verify(reconciliationCache, times(1)).joinWorkerSet(any());
    verify(reconciliationCache, times(1)).leaveWorkerSet(any());
    verify(reconciliationCache, times(1)).getCachedAccountCount();
    verify(reconciliationCache, times(1)).setCachedAccountCount(eq(ACCOUNT_COUNT));
    verify(reconciliationCache, times(1)).getLastNumber();
    verify(reconciliationCache, times(1)).getWorkerCount(anyLong());
    verify(reconciliationCache, times(1)).setLastNumber(eq(Optional.of(VALID_NUMBER)));
    verify(reconciliationCache, times(1)).isAccelerated();
    verify(reconciliationCache, times(1)).lockActiveWorker(any(), anyLong());
    verify(reconciliationCache, times(1)).unlockActiveWorker(any());

    verifyNoMoreInteractions(accountsManager);
    verifyNoMoreInteractions(reconciliationClient);
    verifyNoMoreInteractions(reconciliationCache);
  }

  @Test
  public void testInProgress() {
    DirectoryReconciler directoryReconciler = new DirectoryReconciler(reconciliationClient, inProgressReconciliationCache, accountsManager);
    directoryReconciler.start(new SynchronousExecutorService());
    directoryReconciler.stop();

    verify(accountsManager, times(1)).getAllFrom(eq(VALID_NUMBER), anyInt());

    verify(reconciliationClient, times(1)).sendChunk(eq(Optional.of(VALID_NUMBER)), any());

    verify(inProgressReconciliationCache, times(1)).cleanUpWorkerSet(anyLong());
    verify(inProgressReconciliationCache, times(1)).joinWorkerSet(any());
    verify(inProgressReconciliationCache, times(1)).leaveWorkerSet(any());
    verify(inProgressReconciliationCache, times(1)).getCachedAccountCount();
    verify(inProgressReconciliationCache, times(1)).getLastNumber();
    verify(inProgressReconciliationCache, times(1)).getWorkerCount(anyLong());
    verify(inProgressReconciliationCache, times(1)).setLastNumber(eq(Optional.absent()));
    verify(inProgressReconciliationCache, times(1)).clearAccelerate();
    verify(inProgressReconciliationCache, times(1)).isAccelerated();
    verify(inProgressReconciliationCache, times(1)).lockActiveWorker(any(), anyLong());
    verify(inProgressReconciliationCache, times(1)).unlockActiveWorker(any());

    verifyNoMoreInteractions(accountsManager);
    verifyNoMoreInteractions(reconciliationClient);
    verifyNoMoreInteractions(inProgressReconciliationCache);
  }

  @Test
  public void testNotFound() {
    DirectoryReconciler directoryReconciler = new DirectoryReconciler(notFoundReconciliationClient, reconciliationCache, notFoundAccountsManager);
    directoryReconciler.start(new SynchronousExecutorService());
    directoryReconciler.stop();

    verify(notFoundAccountsManager, times(1)).getAll(eq(0), anyInt());
    verify(notFoundAccountsManager, times(1)).getCount();

    verify(notFoundReconciliationClient, times(1)).sendChunk(eq(Optional.absent()), any());

    verify(reconciliationCache, times(1)).cleanUpWorkerSet(anyLong());
    verify(reconciliationCache, times(1)).joinWorkerSet(any());
    verify(reconciliationCache, times(1)).leaveWorkerSet(any());
    verify(reconciliationCache, times(1)).getCachedAccountCount();
    verify(reconciliationCache, times(1)).setCachedAccountCount(eq(ACCOUNT_COUNT));
    verify(reconciliationCache, times(1)).getLastNumber();
    verify(reconciliationCache, times(1)).getWorkerCount(anyLong());
    verify(reconciliationCache, times(1)).setLastNumber(eq(Optional.absent()));
    verify(reconciliationCache, times(1)).clearAccelerate();
    verify(reconciliationCache, times(1)).isAccelerated();
    verify(reconciliationCache, times(1)).lockActiveWorker(any(), anyLong());
    verify(reconciliationCache, times(1)).unlockActiveWorker(any());

    verifyNoMoreInteractions(notFoundAccountsManager);
    verifyNoMoreInteractions(notFoundReconciliationClient);
    verifyNoMoreInteractions(reconciliationCache);
  }

}
