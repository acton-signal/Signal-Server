package org.whispersystems.textsecuregcm.tests.storage;

import com.google.common.base.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationRequest;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationResponse;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DirectoryReconciler;
import org.whispersystems.textsecuregcm.storage.DirectoryReconciliationCache;
import org.whispersystems.textsecuregcm.storage.DirectoryReconciliationClient;
import org.whispersystems.textsecuregcm.tests.util.SynchronousExecutorService;

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

  private final DirectoryReconciliationResponse successResponse  = new DirectoryReconciliationResponse(DirectoryReconciliationResponse.Status.OK);
  private final DirectoryReconciliationResponse notFoundResponse = new DirectoryReconciliationResponse(DirectoryReconciliationResponse.Status.MISSING);

  @Before
  public void setup() {
    when(account.getNumber()).thenReturn(VALID_NUMBER);
    when(account.isActive()).thenReturn(true);
    when(inactiveAccount.getNumber()).thenReturn(INACTIVE_NUMBER);
    when(inactiveAccount.isActive()).thenReturn(false);

    when(accountsManager.getAllFrom(eq(Optional.absent()), anyInt())).thenReturn(Arrays.asList(account, inactiveAccount));
    when(accountsManager.getAllFrom(eq(Optional.of(VALID_NUMBER)), anyInt())).thenReturn(Arrays.asList(inactiveAccount));
    when(accountsManager.getAllFrom(eq(Optional.of(INACTIVE_NUMBER)), anyInt())).thenReturn(Collections.emptyList());
    when(accountsManager.getCount()).thenReturn(ACCOUNT_COUNT);

    when(reconciliationClient.sendChunk(any())).thenReturn(successResponse);

    when(reconciliationCache.getLastNumber()).thenReturn(Optional.absent());
    when(reconciliationCache.getCachedAccountCount()).thenReturn(Optional.of(ACCOUNT_COUNT));
    when(reconciliationCache.claimActiveWork(any(), anyLong())).thenReturn(true);
    when(reconciliationCache.isAccelerated()).thenReturn(false);
  }

  @Test
  public void testValid() {
    when(reconciliationCache.getCachedAccountCount()).thenReturn(Optional.absent());

    DirectoryReconciler directoryReconciler = new DirectoryReconciler(reconciliationClient, reconciliationCache, accountsManager);
    directoryReconciler.doPeriodicWork();

    verify(accountsManager, times(1)).getAllFrom(eq(Optional.absent()), anyInt());
    verify(accountsManager, times(1)).getCount();

    ArgumentCaptor<DirectoryReconciliationRequest> request = ArgumentCaptor.forClass(DirectoryReconciliationRequest.class);
    verify(reconciliationClient, times(1)).sendChunk(request.capture());

    assertThat(request.getValue().getFromNumber()).isNull();
    assertThat(request.getValue().getToNumber()).isEqualTo(INACTIVE_NUMBER);
    assertThat(request.getValue().getNumbers()).isEqualTo(Arrays.asList(VALID_NUMBER));

    verify(reconciliationCache, times(1)).getCachedAccountCount();
    verify(reconciliationCache, times(1)).setCachedAccountCount(eq(ACCOUNT_COUNT));
    verify(reconciliationCache, times(1)).getLastNumber();
    verify(reconciliationCache, times(1)).setLastNumber(eq(Optional.of(INACTIVE_NUMBER)));
    verify(reconciliationCache, times(1)).isAccelerated();
    verify(reconciliationCache, times(2)).claimActiveWork(any(), anyLong());

    verifyNoMoreInteractions(accountsManager);
    verifyNoMoreInteractions(reconciliationClient);
    verifyNoMoreInteractions(reconciliationCache);
  }

  @Test
  public void testInProgress() {
    when(reconciliationCache.getLastNumber()).thenReturn(Optional.of(VALID_NUMBER));

    DirectoryReconciler directoryReconciler = new DirectoryReconciler(reconciliationClient, reconciliationCache, accountsManager);
    directoryReconciler.doPeriodicWork();

    verify(accountsManager, times(1)).getAllFrom(eq(Optional.of(VALID_NUMBER)), anyInt());

    ArgumentCaptor<DirectoryReconciliationRequest> request = ArgumentCaptor.forClass(DirectoryReconciliationRequest.class);
    verify(reconciliationClient, times(1)).sendChunk(request.capture());

    assertThat(request.getValue().getFromNumber()).isEqualTo(VALID_NUMBER);
    assertThat(request.getValue().getToNumber()).isEqualTo(INACTIVE_NUMBER);
    assertThat(request.getValue().getNumbers()).isEqualTo(Collections.emptyList());

    verify(reconciliationCache, times(1)).getCachedAccountCount();
    verify(reconciliationCache, times(1)).getLastNumber();
    verify(reconciliationCache, times(1)).setLastNumber(eq(Optional.of(INACTIVE_NUMBER)));
    verify(reconciliationCache, times(1)).isAccelerated();
    verify(reconciliationCache, times(2)).claimActiveWork(any(), anyLong());

    verifyNoMoreInteractions(accountsManager);
    verifyNoMoreInteractions(reconciliationClient);
    verifyNoMoreInteractions(reconciliationCache);
  }

  @Test
  public void testLastChunk() {
    when(reconciliationCache.getLastNumber()).thenReturn(Optional.of(INACTIVE_NUMBER));

    DirectoryReconciler directoryReconciler = new DirectoryReconciler(reconciliationClient, reconciliationCache, accountsManager);
    directoryReconciler.doPeriodicWork();

    verify(accountsManager, times(1)).getAllFrom(eq(Optional.of(INACTIVE_NUMBER)), anyInt());

    ArgumentCaptor<DirectoryReconciliationRequest> request = ArgumentCaptor.forClass(DirectoryReconciliationRequest.class);
    verify(reconciliationClient, times(1)).sendChunk(request.capture());

    assertThat(request.getValue().getFromNumber()).isEqualTo(INACTIVE_NUMBER);
    assertThat(request.getValue().getToNumber()).isNull();
    assertThat(request.getValue().getNumbers()).isEqualTo(Collections.emptyList());

    verify(reconciliationCache, times(1)).getCachedAccountCount();
    verify(reconciliationCache, times(1)).getLastNumber();
    verify(reconciliationCache, times(1)).setLastNumber(eq(Optional.absent()));
    verify(reconciliationCache, times(1)).clearAccelerate();
    verify(reconciliationCache, times(1)).isAccelerated();
    verify(reconciliationCache, times(2)).claimActiveWork(any(), anyLong());

    verifyNoMoreInteractions(accountsManager);
    verifyNoMoreInteractions(reconciliationClient);
    verifyNoMoreInteractions(reconciliationCache);
  }

  @Test
  public void testNotFound() {
    when(reconciliationClient.sendChunk(any())).thenReturn(notFoundResponse);

    DirectoryReconciler directoryReconciler = new DirectoryReconciler(reconciliationClient, reconciliationCache, accountsManager);
    directoryReconciler.doPeriodicWork();

    verify(accountsManager, times(1)).getAllFrom(eq(Optional.absent()), anyInt());

    ArgumentCaptor<DirectoryReconciliationRequest> request = ArgumentCaptor.forClass(DirectoryReconciliationRequest.class);
    verify(reconciliationClient, times(1)).sendChunk(request.capture());

    assertThat(request.getValue().getFromNumber()).isNull();
    assertThat(request.getValue().getToNumber()).isEqualTo(INACTIVE_NUMBER);
    assertThat(request.getValue().getNumbers()).isEqualTo(Arrays.asList(VALID_NUMBER));

    verify(reconciliationCache, times(1)).getCachedAccountCount();
    verify(reconciliationCache, times(1)).getLastNumber();
    verify(reconciliationCache, times(1)).setLastNumber(eq(Optional.absent()));
    verify(reconciliationCache, times(1)).clearAccelerate();
    verify(reconciliationCache, times(1)).claimActiveWork(any(), anyLong());

    verifyNoMoreInteractions(accountsManager);
    verifyNoMoreInteractions(reconciliationClient);
    verifyNoMoreInteractions(reconciliationCache);
  }

}
